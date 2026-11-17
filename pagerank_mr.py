import getpass
import math
import os
import subprocess
import sys
from datetime import datetime
from itertools import groupby
from pathlib import Path

DAMPING = 0.85
ROUNDS = 10


def initialize(lines):
    graph = {}
    for line in lines:
        if not line.strip():
            continue
        page, tab, outgoing = line.rstrip("\r\n").partition("\t")
        page = page.strip()
        if not tab or not page:
            raise ValueError("Expected a page name followed by a tab")
        targets = {link.strip() for link in outgoing.split(",") if link.strip()}
        graph.setdefault(page, set()).update(targets)
        for target in targets:
            graph.setdefault(target, set())

    if not graph:
        raise ValueError("The graph is empty")
    return "".join(
        f"{page}\t{1 / len(graph)}\t{','.join(sorted(graph[page]))}\n"
        for page in sorted(graph)
    )


def read_state(lines):
    for line in lines:
        page, rank, outgoing = line.rstrip("\r\n").split("\t", 2)
        yield page, float(rank), outgoing


def map_rank(lines):
    for page, rank, outgoing in read_state(lines):
        print(f"{page}\tlinks\t{outgoing}")
        if outgoing:
            targets = outgoing.split(",")
            contribution = rank / len(targets)
            for target in targets:
                print(f"{target}\tcontribution\t{contribution}")


def reduce_rank(lines, base):
    records = (line.rstrip("\r\n").split("\t", 2) for line in lines)
    for page, group in groupby(records, key=lambda record: record[0]):
        outgoing = ""
        contributions = []
        for _, tag, value in group:
            if tag == "links":
                outgoing = value
            else:
                contributions.append(float(value))
        rank = base + DAMPING * math.fsum(contributions)
        print(f"{page}\t{rank}\t{outgoing}")


def hdfs(*args, **options):
    return subprocess.run(["hdfs", "dfs", *args], text=True, check=True, **options)


def run(graph_path):
    with Path(graph_path).open(encoding="utf-8") as graph:
        state = initialize(graph)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    work = f"/user/{getpass.getuser()}/pagerank-mr-{stamp}-{os.getpid()}"
    source = Path(__file__).resolve()
    print(f"HDFS run directory: {work}", flush=True)
    hdfs("-mkdir", "-p", work)
    previous = f"{work}/round-00.tsv"
    hdfs("-put", "-", previous, input=state)

    for step in range(1, ROUNDS + 1):
        rows = list(read_state(state.splitlines()))
        dangling = math.fsum(rank for _, rank, links in rows if not links)
        base = (1 - DAMPING + DAMPING * dangling) / len(rows)
        output = f"{work}/round-{step:02d}"
        subprocess.run([
            "mapred", "streaming",
            "-D", "mapreduce.framework.name=yarn",
            "-D", f"mapreduce.job.name=PageRank round {step:02d}",
            "-D", "mapreduce.job.reduces=1",
            "-files", source.as_uri(),
            "-input", previous,
            "-output", output,
            "-mapper", f"python3 {source.name} map",
            "-reducer", f"python3 {source.name} reduce {base}",
        ], check=True)
        state = hdfs("-cat", f"{output}/part-*", capture_output=True).stdout
        previous = output
        print(f"Round {step:02d}/{ROUNDS} completed", flush=True)

    result = "".join(
        f"({page},{rank:.10f})\n" for page, rank, _ in read_state(state.splitlines())
    )
    Path("mr-result.txt").write_text(result, encoding="utf-8")
    hdfs("-put", "-", f"{work}/result.txt", input=result)
    print(result, end="")


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "run":
        run(sys.argv[2])
    elif mode == "map":
        map_rank(sys.stdin)
    elif mode == "reduce":
        reduce_rank(sys.stdin, float(sys.argv[2]))
    else:
        raise SystemExit("Usage: pagerank_mr.py run GRAPH")
