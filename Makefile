JAR = GCCacheBenchmark.jar
SRC = GCCacheBenchmark.kt

$(JAR): $(SRC)
	kotlinc $(SRC) -include-runtime -d $(JAR)

clean:
	rm -f $(JAR)

.PHONY: clean
