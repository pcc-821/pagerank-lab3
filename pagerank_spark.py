import sys
from operator import add

from pyspark import SparkContext

DAMPING = 0.85
ROUNDS = 10
PARTITIONS = 2


def vertices(line):
    page, tab, outgoing = line.partition("\t")
    page = page.strip()
    if not tab or not page:
        raise ValueError("Expected a page name followed by a tab")

    targets = {link.strip() for link in outgoing.split(",") if link.strip()}
    yield page, targets
    for target in targets:
        yield target, set()


def contributions(record):
    _, (targets, rank) = record
    if targets:
        share = rank / len(targets)
        for target in targets:
            yield target, share


def run(sc, input_path, output_path):
    links = (
        sc.textFile(input_path)
        .filter(lambda line: bool(line.strip()))
        .flatMap(vertices)
        .reduceByKey(lambda left, right: left | right, PARTITIONS)
        .mapValues(lambda targets: tuple(sorted(targets)))
        .persist()
    )
    node_count = links.count()
    if node_count == 0:
        raise ValueError("The graph is empty")

    print(f"Spark master: {sc.master}, application: {sc.applicationId}", flush=True)
    ranks = links.mapValues(lambda _: 1.0 / node_count).persist()

    for step in range(1, ROUNDS + 1):
        state = links.join(ranks, PARTITIONS).persist()
        dangling = state.values().filter(lambda row: not row[0]).map(lambda row: row[1]).sum()
        base = (1 - DAMPING + DAMPING * dangling) / node_count

        incoming = state.flatMap(contributions).reduceByKey(add, PARTITIONS)
        updated = (
            links.leftOuterJoin(incoming, PARTITIONS)
            .mapValues(lambda row, base=base: base + DAMPING * (row[1] or 0.0))
            .persist()
        )
        updated.count()
        state.unpersist()
        ranks.unpersist()
        ranks = updated
        print(f"Spark round {step:02d}/{ROUNDS} completed", flush=True)

    ranks.sortByKey(numPartitions=1).map(
        lambda row: f"({row[0]},{row[1]:.10f})"
    ).saveAsTextFile(output_path)


if __name__ == "__main__":
    with SparkContext(appName="Lab3 PageRank Spark") as sc:
        sc.setLogLevel("WARN")
        run(sc, sys.argv[1], sys.argv[2])
