mvn clean install -DskipTests=true
mvn package
storm jar target/storm-starter-1.0.2.jar org.apache.storm.starter.Question1 -local local
#storm jar target/storm-starter-1.0.2.jar org.apache.storm.starter.Question1 production-topology remote
