mkdir classes
find -name *.java > sources.txt
javac -d classes/ -cp classes/ @sources.txt
