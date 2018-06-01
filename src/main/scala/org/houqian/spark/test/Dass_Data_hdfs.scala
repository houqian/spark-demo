package org.houqian.spark.test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * Created by finup on 2018/3/30.
  */
object Dass_Data_hdfs extends App {
  val daas_path = "hdfs://192.168.176.62:8020"
  val daas_path_other = "hdfs://192.168.176.61:8020"

  val spark = SparkSession.builder.appName("daas_data_syn")
    //.config("spark.sql.warehouse.dir","hdfs://192.168.176.62:8020/user/hive/warehouse")
    .enableHiveSupport()
    //.master("local[2]")
    .getOrCreate()
 /* spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  spark.sql("set hive.exec.max.dynamic.partitions.pernode=1000")*/
  spark.sql("use limw")
  insert_qianjin_bussiness
  val ff_max_time = get_ff_qianzhan_bussiness_max_time
  insert_qianzhan_business(if(ff_max_time == null) "1970-01-01 00:00:00" else ff_max_time.replace("|",":").replace("_"," "))

  def get_df_by_hdfs(daas_path: String, daas_path_other: String, path: String) = {
    try {
      val qianjin_home_log = spark.read.parquet(s"${daas_path}/user/hive/warehouse/${path}")
      qianjin_home_log
    } catch {
      case e: Exception => {
        print(s"Error : ${daas_path} is standby try to connect by ${daas_path_other}")
        val qianjin_home_log = spark.read.parquet(s"${daas_path_other}/user/hive/warehouse/${path}")
        qianjin_home_log
      }
    }
  }
  //spark.read.parquet("hdfs://192.168.176.62:8020/user/hive/warehouse/cif.db/cif_weixin_bd_reg_trace_ff")
  def get_daas_cif_weixin_bd_reg_trace_ff_max_time()={
    val cif_weixin_bd_reg_trace_ff = get_df_by_hdfs(daas_path,daas_path_other,"cif.db/cif_weixin_bd_reg_trace_ff")
    cif_weixin_bd_reg_trace_ff.createOrReplaceTempView("cif_weixin_bd_reg_trace_ff")
    val select_sql =
      """
        |select max(create_time) create_time from cif_weixin_bd_reg_trace_ff
      """.stripMargin
    val create_time = spark.sql(select_sql)
    val time = create_time.rdd.take(1)(0).getAs[String](0)
    time
  }
  def get_ff_qianzhan_bussiness_max_time()={
    val select_sql =
      """
        |select max(dt) dt from ff_qianzhan_business
      """.stripMargin
    val dt = spark.sql(select_sql)
    val time = dt.rdd.take(1)(0).getAs[String](0)
    time
  }
  def drop_table(table_name : String)={
    val drop_sql = s"drop table if exists ${table_name}"
    spark.sql(drop_sql)

  }
  def create_ff_qianzhan_business()={
    val create_sql = "CREATE  TABLE `ff_qianzhan_business`(apply_time string,`session_id` string, `encrypt_device_num` string, `device_num` string, `mobile` string, `user_id` string, `amount` string, `state` string, `update_time` string) PARTITIONED BY (`dt` string) STORED  AS  parquet"
    spark.sql(create_sql)
  }
  def get_ff_qianzhan_business_partiton_num() :Long={
    val select_sql = "select count(1) from (select dt from ff_qianzhan_business group by dt) a"
    val dt = spark.sql(select_sql)
    val num = dt.rdd.take(1)(0).getAs[Long](0)
    num
  }
  def getNowDate():String={
    val now:Date = new Date()
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val today = dateFormat.format( now )
    today
  }


  def insert_qianjin_bussiness()={
    val qianjin_home_log = get_df_by_hdfs(daas_path, daas_path_other, "cif_qianjin_log_fake_mysql.db/qianjin_home_log")
    val mid_user_invest_detail = get_df_by_hdfs(daas_path, daas_path_other, "cif_mid.db/mid_user_invest_detail")
    val mid_user_detail_temp = get_df_by_hdfs(daas_path, daas_path_other, "cif_mid.db/mid_user_detail_temp")

    qianjin_home_log.createOrReplaceTempView("qianjin_home_log")
    mid_user_detail_temp.createOrReplaceTempView("mid_user_detail_temp")
    mid_user_invest_detail.createOrReplaceTempView("mid_user_invest_detail")

    val qianjin_select_sql =
      """select  a.devicenum,a.userid,d.invest_amt,
        |            parse_url(a.fullurl,'QUERY','sessionId') sessionId,
        |            parse_url(a.fullurl,'QUERY','encryptDeviceNum')  encryptDeviceNum,
        |            a.actiontime
        |            from  qianjin_home_log  a
        |            left join mid_user_detail_temp c on a.userid= c.user_id
        |            left join (
        |            select user_id ,sum(invest_amt) invest_amt
        |                from  mid_user_invest_detail group by user_id
        |            ) d on a.userid=d.user_id
        |            where a.us in ('342_5414_1','5504','5505')   and a.act='reg_success' """.stripMargin

    val aiqianjin_business = spark.sql(qianjin_select_sql)
    aiqianjin_business.cache()
    aiqianjin_business.count()
    aiqianjin_business.createOrReplaceTempView("aiqianjin_business")

    val qianjin_insert_sql = "insert overwrite table ff_aiqianjin_business_test select * from aiqianjin_business"

    spark.sql(qianjin_insert_sql)
  }
  /**
    * 更新表qianzhan_business，增量更新，当分区数大于等于3个的时候，进行overwrite
    * */
  def insert_qianzhan_business(startTime:String) : Int={
    var ff_time = startTime
    //如果分区数量超过一定数量就把数据删除，做一次全量更新
    val partitionNum = get_ff_qianzhan_business_partiton_num
    if(partitionNum >= 3){
      drop_table("ff_qianzhan_business")
      create_ff_qianzhan_business
      ff_time = "1970-01-01 00:00:00"
    }

    val dass_time = get_daas_cif_weixin_bd_reg_trace_ff_max_time
    //如果ff表最大时间与dass表最大时间一致，以是最新状态不必更新
    if(ff_time.equals(dass_time)){
      println("ff_qianzhan_business"," 表中数据已是最新状态，不必更新...")
      return 1
    }
    println("ff_time",ff_time)
    println("dass_time",dass_time)
    val select_sql =
      s"""
        |select  c.update_time as apply_time,a.session_id,a.encrypt_device_num,a.device_num,b.mobile,cast(c.user_id  AS string) user_id,c.amount,cast(c.state AS string) state,a.update_time
        |            from cif_weixin_bd_reg_trace_ff  a
        |            left join user  b on b.mobile=a.mobile
        |            left join  (select  user_id,amount,state,update_time
        |                        from withdraw_apply
        |                        ) c on b.id=c.user_id
        |            where 1=1
        |            and a.qd like 'ffmj%'
        |            and a.create_time >= '${ff_time}'
      """.stripMargin

    val cif_weixin_bd_reg_trace_ff = get_df_by_hdfs(daas_path,daas_path_other,"cif.db/cif_weixin_bd_reg_trace_ff")
    val user = get_df_by_hdfs(daas_path,daas_path_other,"cif_nirvana.db/user")
    val withdraw_apply = get_df_by_hdfs(daas_path,daas_path_other,"cif_nirvana.db/withdraw_apply")

    cif_weixin_bd_reg_trace_ff.createOrReplaceTempView("cif_weixin_bd_reg_trace_ff")
    user.createOrReplaceTempView("user")
    withdraw_apply.createOrReplaceTempView("withdraw_apply")

    val ff_qianzhan_business = spark.sql(select_sql)
    ff_qianzhan_business.createOrReplaceTempView("temp_ff_qianzhan_business")
    ff_qianzhan_business.show()
   // val dt = ff_qianzhan_business_select
    val qianzhan_insert_sql = s"insert overwrite table ff_qianzhan_business partition(dt='${dass_time.replace(":","|").replace(" ","_")}') select distinct * from temp_ff_qianzhan_business"
    spark.sql(qianzhan_insert_sql)
    //检查daas与ff数据是否一致
    qz_data_exam
    /*try{
      spark.sql(s"alter table ff_qianzhan_business add partition(dt='${dass_time.replace(":","|").replace(" ","_")}')")
    }catch {
      case e : Exception =>
        println("分区已存在...")
    }*/
    return 0
  }

  def qz_data_exam(): Unit ={
    val daas_sql =
      s"""
         |select distinct c.update_time as apply_time,a.session_id,a.encrypt_device_num,a.device_num,b.mobile,cast(c.user_id  AS string) user_id,c.amount,cast(c.state AS string) state,a.update_time
         |            from cif_weixin_bd_reg_trace_ff  a
         |            left join user  b on b.mobile=a.mobile
         |            left join  (select  user_id,amount,state,update_time
         |                        from withdraw_apply
         |                        ) c on b.id=c.user_id
         |            where 1=1
         |            and a.qd like 'ffmj%'
         |
      """.stripMargin
    val daas_num = spark.sql(daas_sql).count()
    val ff_sql =
      s"""
         |select distinct * from ff_qianzhan_business
       """.stripMargin
    val ff_num = spark.sql(ff_sql).count()
    println("dass_num :"+daas_num)
    println("ff_num :"+ff_num)
  }
}
