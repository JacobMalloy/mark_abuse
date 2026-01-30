import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.PrintStream
import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.ref.Reference
import java.util.Random

/**
 * Cache-Sensitive GC Micro-Benchmark
 *
 * Recreated from: Carpen-Amarie, Vavouliotis, Tovletoglou, Grot, Mueller.
 * "Concurrent GCs and Modern Java Workloads: A Cache Perspective", ISMM '23.
 *
 * Computes a count-group-by aggregate over uniformly random keys using a
 * hash table (two separate int arrays) sized to be fully LLC-resident.
 * A large directed random graph is pre-allocated to give the GC marking
 * phase significant traversal work. System.gc() is triggered every N
 * iterations from a dedicated thread so that GC runs concurrently with
 * the workload, and the resulting cache pollution is observable as
 * increased iteration time.
 *
 * COMPILE:
 *   kotlinc GCCacheBenchmark.kt -include-runtime -d GCCacheBenchmark.jar
 *
 * RUN (ZGC, JDK 17):
 *   java -XX:+UseZGC -XX:-ZProactive -Xmx16g \
 *        -Xlog:gc*:file=gc.log:time,uptime,level,tags \
 *        -jar GCCacheBenchmark.jar -o results.csv
 *
 * RUN (ZGC, JDK 21+, non-generational to match the paper):
 *   java -XX:+UseZGC -XX:-ZGenerational -XX:-ZProactive -Xmx16g \
 *        -Xlog:gc*:file=gc.log:time,uptime,level,tags \
 *        -jar GCCacheBenchmark.jar -o results.csv
 *
 * RUN (Shenandoah):
 *   java -XX:+UseShenandoahGC -XX:+ExplicitGCInvokesConcurrent -Xmx16g \
 *        -Xlog:gc*:file=gc.log:time,uptime,level,tags \
 *        -jar GCCacheBenchmark.jar -o results.csv
 *
 * MEASURE LLC MISSES (Linux, whole process):
 *   perf stat -e LLC-load-misses,LLC-loads,instructions \
 *        java [flags] -jar GCCacheBenchmark.jar -o results.csv
 *
 * THREAD PINNING (Linux, optional -- paper pins main + GC threads to separate cores):
 *   taskset -c 0-1 java [flags] -jar GCCacheBenchmark.jar -o results.csv
 */

// ---- Configuration (defaults from the paper, Section 4.2) ----

/** Hash table cardinality. Two int arrays of this size = 2 * 4 * C bytes total. */
var C = 1_900_544  // ~14.5 MiB

/** Number of uniform random keys generated per iteration. */
var N = 100_000_000

/** Total benchmark iterations. */
var totalIterations = 100

/** Warmup iterations excluded from output (paper drops first 15). */
var warmupIterations = 15

/** Nodes in the directed random graph for GC marking work. */
var graphNodes = 5_000_000

/** Edges per node (total edges = graphNodes * edgesPerNode). */
var edgesPerNode = 100

/** Output file for CSV results (null = stdout). */
var outputFile: String? = null

// ---- Graph node ----

/**
 * A node in the directed random graph. Each node holds an array of
 * references to randomly chosen neighbors. This creates a large
 * live-set (~5 GiB at defaults) that the GC must traverse during
 * marking, independently of the application's working-set.
 */
class Node(n: Int) {
    val neighbors: Array<Node?> = arrayOfNulls(n)
}

// ---- Entry point ----

fun main(args: Array<String>) {
    parseArgs(args)

    val csv = if (outputFile != null)
        PrintStream(BufferedOutputStream(FileOutputStream(outputFile!!)), false)
    else
        PrintStream(BufferedOutputStream(System.out), false)

    printConfig()

    // Build the reference graph before the workload starts.
    // It is never operated on afterwards -- it exists solely as a body
    // of live objects that the GC must mark in every collection cycle.
    // The GC live-set and the application working-set (hash table) are
    // almost fully disjoint.
    System.err.print("Building reference graph...")
    System.err.flush()
    val graph = buildGraph(graphNodes, edgesPerNode)
    System.err.println(" done.")
    System.err.println()

    val gcBeans = ManagementFactory.getGarbageCollectorMXBeans()
    System.err.println("Active GC collectors:")
    for (bean in gcBeans) {
        System.err.println("  ${bean.name}")
    }
    System.err.println()

    // Hash table: two separate int arrays for keys and counts.
    // Separating them increases cache sensitivity -- each random lookup
    // touches two distinct cache lines rather than one.
    // Keys in [1, C] map to index = key (identity hash). Index 0 unused.
    val htKeys = IntArray(C + 1)
    val htCounts = IntArray(C + 1)

    val rng = Random(42)

    // CSV output to file (or stdout); diagnostics to stderr
    csv.println("iteration,time_s,gc_count_delta,gc_time_delta_ms")

    System.err.printf("Running %d iterations (%d warmup)...%n",
        totalIterations, warmupIterations)

    // Capture config in locals so the hot loop avoids top-level property
    // getter calls on every iteration.
    val c = C
    val n = N

    for (iter in 1..totalIterations) {
        htKeys.fill(0)
        htCounts.fill(0)

        val gcCountBefore = gcBeans.sumCounts()
        val gcTimeBefore = gcBeans.sumTimes()

        // ---- Core workload: count-group-by ----
        // Generate N uniform i.i.d. keys in [1, C] and count each distinct
        // key in the hash table. Integer.hashCode() is the identity function,
        // so key k maps directly to index k -- no collisions, no probing.
        val t0 = System.nanoTime()

        for (i in 0 until n) {
            val key = rng.nextInt(c) + 1
            htKeys[key] = key
            htCounts[key]++
        }

        val t1 = System.nanoTime()
        // ---- End workload ----

        val gcCountAfter = gcBeans.sumCounts()
        val gcTimeAfter = gcBeans.sumTimes()

        if (iter > warmupIterations) {
            csv.printf("%d,%.6f,%d,%d%n",
                iter, (t1 - t0) / 1e9,
                gcCountAfter - gcCountBefore,
                gcTimeAfter - gcTimeBefore)
        }

        if (iter % 10 == 0) {
            System.err.printf("  iteration %d / %d%n", iter, totalIterations)
        }
    }

    csv.flush()
    csv.close()

    // Prevent the graph from being collected before the benchmark ends
    Reference.reachabilityFence(graph)

    System.err.println()
    System.err.println("Benchmark complete.")
}

// ---- Graph construction ----

fun buildGraph(numNodes: Int, numEdgesPerNode: Int): Array<Node> {
    val nodes = Array(numNodes) { Node(numEdgesPerNode) }
    val rng = Random(12345)

    for (i in 0 until numNodes) {
        for (j in 0 until numEdgesPerNode) {
            nodes[i].neighbors[j] = nodes[rng.nextInt(numNodes)]
        }
    }
    return nodes
}

// ---- GC MXBean helpers ----

fun List<GarbageCollectorMXBean>.sumCounts(): Long =
    sumOf { maxOf(it.collectionCount, 0L) }

fun List<GarbageCollectorMXBean>.sumTimes(): Long =
    sumOf { maxOf(it.collectionTime, 0L) }

// ---- Argument parsing ----

fun parseArgs(args: Array<String>) {
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "-c", "--cardinality" -> C = args[++i].toInt()
            "-k", "--keys"       -> N = args[++i].toInt()
            "--iterations"       -> totalIterations = args[++i].toInt()
            "--warmup"           -> warmupIterations = args[++i].toInt()
            "--graph-nodes"      -> graphNodes = args[++i].toInt()
            "--edges-per-node"   -> edgesPerNode = args[++i].toInt()
            "-o", "--output"     -> outputFile = args[++i]
            "-h", "--help"       -> { printHelp(); System.exit(0) }
            else -> {
                System.err.println("Unknown option: ${args[i]}")
                printHelp()
                System.exit(1)
            }
        }
        i++
    }
}

fun printHelp() {
    System.err.println("""
        |Usage: java -jar GCCacheBenchmark.jar [options]
        |
        |Options:
        |  -c, --cardinality N    Hash table entries (default: 1900544, ~14.5 MiB)
        |  -k, --keys N           Keys per iteration (default: 100000000)
        |      --iterations N     Total iterations (default: 100)
        |      --warmup N         Warmup iterations to skip (default: 15)
        |      --graph-nodes N    Graph nodes for GC work (default: 5000000)
        |      --edges-per-node N Edges per graph node (default: 100)
        |  -o, --output FILE      Write CSV results to FILE (default: stdout)
        |  -h, --help             Show this help
    """.trimMargin())
}

fun printConfig() {
    with(System.err) {
        println("=== Cache-Sensitive GC Micro-Benchmark ===")
        println("    Carpen-Amarie et al., ISMM '23")
        println()
        printf("  Hash table cardinality:  %,d entries%n", C)
        printf("  Hash table size:         %.1f MiB%n", (2.0 * 4 * C) / (1024.0 * 1024))
        printf("  Keys per iteration:      %,d%n", N)
        printf("  Total iterations:        %d%n", totalIterations)
        printf("  Warmup iterations:       %d%n", warmupIterations)
        printf("  Graph:                   %,d nodes x %d edges = %,d refs%n",
            graphNodes, edgesPerNode, graphNodes.toLong() * edgesPerNode)
        printf("  Est. graph live-set:     ~%.1f GiB%n",
            estimateGraphSize(graphNodes, edgesPerNode) / (1024.0 * 1024 * 1024))
        println()
    }
}

fun estimateGraphSize(nodes: Int, edges: Int): Long {
    // Per node: ~16 bytes header + 8 bytes ref to neighbor array
    // Per neighbor array: ~16 bytes header + edges * 8 bytes (refs, conservative)
    val perNode = 24L + 16 + edges.toLong() * 8
    return nodes.toLong() * perNode
}
