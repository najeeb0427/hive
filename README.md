# hive
$ is just indicates relative cells in excel, so you can ignore them

wget http://central.maven.org/maven2/org/apache/hive/hcatalog/hive-hcatalog-core/0.13.0/hive-hcatalog-core-0.13.0.jar

add jar hive-hcatalog-core-0.13.0.jar

CREATE EXTERNAL TABLE tweets (  createddate string,  geolocation string,  tweetmessage string,  `user` struct<geoenabled:boolean, id:int, name:string, screenname:string, userlocation:string>) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'


sampple data :
 {  "user": {    "userlocation": "California, Santa Clara",    "id": 222222,    "name": "Hortonworks",    "screenname": "hortonworks",    "geoenabled": true  },  "tweetmessage": "Learn more about #Spark in #HDP 2.3 with @Hortonworks founder @acmurthy in this video overview http://bit.ly/1gOyr9w  #hadoop",  "createddate": "2015-07-24T16:30:33",  "geolocation": null}
  
   
    
link :
https://community.hortonworks.com/questions/28684/creating-a-hive-table-with-orgapachehcatalogdatajs.html


