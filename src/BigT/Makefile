# BIGT make file

# modify the paths appropriately
JDKPATH = /Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

Menu:Menu.java
	$(JAVAC) Menu.java

menu: Menu
	$(JAVA) BigT.Menu





