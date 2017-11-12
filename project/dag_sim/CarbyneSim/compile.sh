mkdir classes
find -name *.java > sources.txt
javac -d classes/ -cp "classes/:jars/json-simple-1.1.1.jar" @sources.txt
