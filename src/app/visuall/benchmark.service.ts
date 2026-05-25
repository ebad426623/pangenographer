import { Injectable } from "@angular/core";
import {
  DbResponseType,
  GraphResponse,
  Neo4jEdgeDirection,
  TableResponse,
} from "./db-service/data-types";
import { Neo4jDb } from "./db-service/neo4j-db.service";
import { CytoscapeService } from "./cytoscape.service";
import { GlobalVariableService } from "./global-variable.service";

export type BenchmarkCaseKey =
  | "database-summary"
  | "top-degree"
  | "cypher-edge-sample"
  | "cypher-neighborhood"
  | "pg2-neighborhood"
  | "pg2-graph-of-interest"
  | "pg2-common-stream"
  | "custom-name-search"
  | "custom-sequence-search"
  | "custom-sequence-chain";

export interface BenchmarkCaseDefinition {
  key: BenchmarkCaseKey;
  label: string;
  rendersGraph: boolean;
}

export const BENCHMARK_CASES: BenchmarkCaseDefinition[] = [
  {
    key: "database-summary",
    label: "Cypher: database summary",
    rendersGraph: false,
  },
  {
    key: "top-degree",
    label: "Cypher: top degree segments",
    rendersGraph: false,
  },
  {
    key: "cypher-edge-sample",
    label: "Cypher: edge sample render",
    rendersGraph: true,
  },
  {
    key: "cypher-neighborhood",
    label: "Cypher: seed neighborhood render",
    rendersGraph: true,
  },
  {
    key: "pg2-neighborhood",
    label: "PG2 procedure: neighborhood",
    rendersGraph: true,
  },
  {
    key: "pg2-graph-of-interest",
    label: "PG2 procedure: graph of interest",
    rendersGraph: true,
  },
  {
    key: "pg2-common-stream",
    label: "PG2 procedure: common stream",
    rendersGraph: true,
  },
  {
    key: "custom-name-search",
    label: "Custom: search by segment name",
    rendersGraph: true,
  },
  {
    key: "custom-sequence-search",
    label: "Custom: search by segment sequence",
    rendersGraph: true,
  },
  {
    key: "custom-sequence-chain",
    label: "Custom: search by sequence chain",
    rendersGraph: true,
  },
];

export interface BenchmarkOptions {
  warmupRuns: number;
  measuredRuns: number;
  graphLimits: number[];
  neighborhoodRadii: number[];
  procedurePageSize: number;
  cypherPathLimit: number;
  seedCount: number;
  seedSegmentNames: string[];
  selectedCases: BenchmarkCaseKey[];
  layoutTimeoutMs: number;
  customNameSampleCount: number;
  customSequenceLengths: number[];
  customChainNodeCount: number;
  customChainMaxJumpLengths: number[];
  customChainMinSubsequenceMatchLength: number;
  useProcedurePool: boolean;
  procedurePoolRadius: number;
  procedurePoolMinNodes: number;
}

export interface BenchmarkRunResult {
  caseKey: BenchmarkCaseKey;
  caseLabel: string;
  params: string;
  runIndex: number;
  measured: boolean;
  status: "ok" | "error" | "cancelled";
  queryMs?: number;
  conversionMs?: number;
  renderSetupMs?: number;
  layoutQueueMs?: number;
  layoutMs?: number;
  totalMs?: number;
  rows?: number;
  nodes?: number;
  edges?: number;
  payloadKb?: number;
  browserHeapMb?: number;
  layoutTimedOut?: boolean;
  seedIds?: string;
  seedNames?: string;
  error?: string;
}

interface BenchmarkSeed {
  id: string;
  segmentName: string;
  degree?: number;
}

interface CustomSequenceSample {
  length: number;
  substring: string;
  sourceName: string;
}

interface CustomChainSample {
  maxJumpLength: number;
  minSubsequenceMatchLength: number;
  sequences: string[];
  chainNodeNames: string[];
}

interface CustomQuerySamples {
  names: string[];
  sequenceSamples: CustomSequenceSample[];
  chainSamples: CustomChainSample[];
}

interface ProcedurePoolNode {
  id: string;
  segmentName: string;
  distance: number;
}

interface BuiltBenchmarkCase {
  key: BenchmarkCaseKey;
  label: string;
  params: string;
  responseType: DbResponseType;
  isTimeboxed: boolean;
  renderGraph: boolean;
  query: string;
  toGraph?: (response: any) => GraphResponse;
  seedsUsed?: BenchmarkSeed[];
}

interface RenderTiming {
  renderSetupMs?: number;
  layoutQueueMs?: number;
  layoutMs?: number;
  totalMs?: number;
  layoutTimedOut?: boolean;
}

@Injectable({
  providedIn: "root",
})
export class BenchmarkService {
  private isCancelled = false;

  constructor(
    private _db: Neo4jDb,
    private _cyService: CytoscapeService,
    private _g: GlobalVariableService,
  ) {}

  getDefaultOptions(): BenchmarkOptions {
    return {
      warmupRuns: 1,
      measuredRuns: 5,
      graphLimits: [150, 500, 1000, 2500],
      neighborhoodRadii: [2, 4, 6],
      procedurePageSize: 2500,
      cypherPathLimit: 2500,
      seedCount: 2,
      seedSegmentNames: [],
      selectedCases: BENCHMARK_CASES.map((x) => x.key),
      layoutTimeoutMs: 180000,
      customNameSampleCount: 5,
      customSequenceLengths: [5, 10, 20, 50, 100],
      customChainNodeCount: 10,
      customChainMaxJumpLengths: [0, 2],
      customChainMinSubsequenceMatchLength: 2,
      useProcedurePool: true,
      procedurePoolRadius: 3,
      procedurePoolMinNodes: 20,
    };
  }

  stop(): void {
    this.isCancelled = true;
  }

  async runSuite(
    options: BenchmarkOptions,
    onResult?: (result: BenchmarkRunResult) => void,
  ): Promise<BenchmarkRunResult[]> {
    this.isCancelled = false;
    const results: BenchmarkRunResult[] = [];
    const seeds = await this.resolveSeeds(options);
    const customSamples = await this.resolveCustomQuerySamples(options);
    const procedureSeeds = await this.resolveProcedureSeeds(options, seeds);
    const cases = this.buildCases(
      options,
      seeds,
      customSamples,
      procedureSeeds,
    );
    const totalRuns = options.warmupRuns + options.measuredRuns;

    for (const benchmarkCase of cases) {
      for (let i = 0; i < totalRuns; i++) {
        if (this.isCancelled) {
          const cancelled = this.createCancelledResult(benchmarkCase, i);
          results.push(cancelled);
          onResult?.(cancelled);
          return results;
        }

        const result = await this.runCase(
          benchmarkCase,
          i,
          i >= options.warmupRuns,
          seeds,
          options,
        );
        results.push(result);
        onResult?.(result);
      }
    }

    return results;
  }

  resultsToCsv(results: BenchmarkRunResult[]): string {
    const columns = [
      "caseKey",
      "caseLabel",
      "params",
      "runIndex",
      "measured",
      "status",
      "queryMs",
      "conversionMs",
      "renderSetupMs",
      "layoutQueueMs",
      "layoutMs",
      "totalMs",
      "rows",
      "nodes",
      "edges",
      "payloadKb",
      "browserHeapMb",
      "layoutTimedOut",
      "seedIds",
      "seedNames",
      "error",
    ];
    const lines = [columns.join(",")];
    for (const result of results) {
      lines.push(
        columns
          .map((column) => this.csvCell((result as any)[column]))
          .join(","),
      );
    }
    return lines.join("\n");
  }

  downloadCsv(results: BenchmarkRunResult[]): void {
    const csv = this.resultsToCsv(results);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pg2-web-benchmark-${new Date()
      .toISOString()
      .replace(/[:.]/g, "-")}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  private async runCase(
    benchmarkCase: BuiltBenchmarkCase,
    runIndex: number,
    measured: boolean,
    seeds: BenchmarkSeed[],
    options: BenchmarkOptions,
  ): Promise<BenchmarkRunResult> {
    const totalStart = performance.now();
    const seedsForRow = benchmarkCase.seedsUsed ?? seeds;
    const baseResult: BenchmarkRunResult = {
      caseKey: benchmarkCase.key,
      caseLabel: benchmarkCase.label,
      params: benchmarkCase.params,
      runIndex,
      measured,
      status: "ok",
      seedIds: seedsForRow.map((x) => x.id).join(";"),
      seedNames: seedsForRow.map((x) => x.segmentName).join(";"),
    };

    try {
      const queryStart = performance.now();
      const response = await this._db.runQueryPromised(
        benchmarkCase.query,
        benchmarkCase.responseType,
        benchmarkCase.isTimeboxed,
      );
      baseResult.queryMs = performance.now() - queryStart;
      baseResult.payloadKb = this.payloadKb(response);

      let graph: GraphResponse | undefined;
      let rows: number | undefined;
      if (benchmarkCase.toGraph) {
        const conversionStart = performance.now();
        graph = benchmarkCase.toGraph(response);
        baseResult.conversionMs = performance.now() - conversionStart;
      } else if (benchmarkCase.responseType === DbResponseType.graph) {
        graph = response as GraphResponse;
      } else {
        rows = this.getTableRowCount(response as TableResponse);
      }

      if (graph) {
        baseResult.nodes = graph.nodes.length;
        baseResult.edges = graph.edges.length;
      }
      if (rows !== undefined) {
        baseResult.rows = rows;
      }

      if (benchmarkCase.renderGraph && graph && graph.nodes.length > 0) {
        const renderTiming = await this.renderGraph(graph, options);
        Object.assign(baseResult, renderTiming);
      }

      baseResult.browserHeapMb = this.browserHeapMb();
      baseResult.totalMs = performance.now() - totalStart;
      return baseResult;
    } catch (e) {
      baseResult.status = "error";
      baseResult.error = this.errorMessage(e);
      baseResult.totalMs = performance.now() - totalStart;
      baseResult.browserHeapMb = this.browserHeapMb();
      return baseResult;
    }
  }

  private createCancelledResult(
    benchmarkCase: BuiltBenchmarkCase,
    runIndex: number,
  ): BenchmarkRunResult {
    return {
      caseKey: benchmarkCase.key,
      caseLabel: benchmarkCase.label,
      params: benchmarkCase.params,
      runIndex,
      measured: false,
      status: "cancelled",
    };
  }

  private buildCases(
    options: BenchmarkOptions,
    seeds: BenchmarkSeed[],
    customSamples: CustomQuerySamples,
    procedureSeeds: BenchmarkSeed[] | null,
  ): BuiltBenchmarkCase[] {
    const selected = new Set(options.selectedCases);
    const cases: BuiltBenchmarkCase[] = [];
    const seedIds = seeds.map((x) => this.cypherString(x.id));
    const firstSeedId = seedIds[0];
    const dbTimeout = this._g.userPreferences.dbTimeout.getValue() * 1000;
    const cypherLimit = options.cypherPathLimit;
    const procedurePageSize = options.procedurePageSize;
    const procSeedsResolved =
      procedureSeeds && procedureSeeds.length > 0 ? procedureSeeds : seeds;
    const procSeedIds = procSeedsResolved.map((x) => this.cypherString(x.id));

    if (selected.has("database-summary")) {
      cases.push({
        key: "database-summary",
        label: "Cypher: database summary",
        params: "counts",
        responseType: DbResponseType.table,
        isTimeboxed: false,
        renderGraph: false,
        query: `
          MATCH (s:SEGMENT)
          WITH count(s) AS segments
          OPTIONAL MATCH ()-[l:LINK]->()
          WITH segments, count(l) AS links
          OPTIONAL MATCH ()-[j:JUMP]->()
          WITH segments, links, count(j) AS jumps
          OPTIONAL MATCH ()-[c:CONTAINMENT]->()
          RETURN segments, links, jumps, count(c) AS containments
        `,
      });
    }

    if (selected.has("top-degree")) {
      cases.push({
        key: "top-degree",
        label: "Cypher: top degree segments",
        params: "limit=100",
        responseType: DbResponseType.table,
        isTimeboxed: false,
        renderGraph: false,
        query: `
          MATCH (s:SEGMENT)-[r]-()
          WITH s, count(r) AS degree
          RETURN elementId(s) AS id,
                 s.segmentName AS segmentName,
                 s.segmentLength AS segmentLength,
                 degree
          ORDER BY degree DESC
          LIMIT 100
        `,
      });
    }

    if (selected.has("cypher-edge-sample")) {
      for (const limit of options.graphLimits) {
        cases.push({
          key: "cypher-edge-sample",
          label: "Cypher: edge sample render",
          params: `limit=${limit}`,
          responseType: DbResponseType.graph,
          isTimeboxed: false,
          renderGraph: true,
          query: `
            MATCH (s:SEGMENT)-[e]->(t:SEGMENT)
            RETURN s, e, t
            LIMIT ${limit}
          `,
        });
      }
    }

    if (selected.has("cypher-neighborhood") && firstSeedId) {
      for (const radius of options.neighborhoodRadii) {
        cases.push({
          key: "cypher-neighborhood",
          label: "Cypher: seed neighborhood render",
          params: `radius=${radius}; pathLimit=${options.cypherPathLimit}`,
          responseType: DbResponseType.graph,
          isTimeboxed: false,
          renderGraph: true,
          query: `
            MATCH (startNode)
            WHERE elementId(startNode) = ${firstSeedId}
            MATCH path = (startNode)-[*1..${radius}]-(endNode)
            RETURN path
            LIMIT ${options.cypherPathLimit}
          `,
        });
      }
    }

    if (selected.has("pg2-neighborhood") && seedIds.length > 0) {
      for (const radius of options.neighborhoodRadii) {
        cases.push({
          key: "pg2-neighborhood",
          label: "PG2 procedure: neighborhood",
          params: `radius=${radius}; pageSize=${options.procedurePageSize}`,
          responseType: DbResponseType.table,
          isTimeboxed: false,
          renderGraph: true,
          toGraph: this.tableResponseToGraph.bind(this),
          query: `
            CALL neighborhood([${seedIds.join(",")}], [], ${radius}, true,
              ${options.procedurePageSize}, 1, '', false, null, 2,
              {}, 0, 0, 0, ${dbTimeout}, null)
          `,
        });
      }
    }

    if (selected.has("pg2-graph-of-interest") && procSeedIds.length > 0) {
      const pooled = procSeedsResolved !== seeds;
      for (const radius of options.neighborhoodRadii) {
        cases.push({
          key: "pg2-graph-of-interest",
          label: "PG2 procedure: graph of interest",
          params: `length=${radius}; pageSize=${options.procedurePageSize}; seeds=${pooled ? "pool" : "top-degree"}`,
          responseType: DbResponseType.table,
          isTimeboxed: false,
          renderGraph: true,
          toGraph: this.tableResponseToGraph.bind(this),
          seedsUsed: procSeedsResolved,
          query: `
            CALL graphOfInterest([${procSeedIds.join(",")}], [], ${radius}, true,
              ${options.procedurePageSize}, 1, '', false, null, 2,
              {}, 0, 0, 0, ${dbTimeout}, null)
          `,
        });
      }
    }

    if (selected.has("pg2-common-stream") && procSeedIds.length > 1) {
      const pooled = procSeedsResolved !== seeds;
      for (const radius of options.neighborhoodRadii) {
        cases.push({
          key: "pg2-common-stream",
          label: "PG2 procedure: common stream",
          params: `length=${radius}; pageSize=${options.procedurePageSize}; seeds=${pooled ? "pool" : "top-degree"}`,
          responseType: DbResponseType.table,
          isTimeboxed: false,
          renderGraph: true,
          toGraph: this.tableResponseToGraph.bind(this),
          seedsUsed: procSeedsResolved,
          query: `
            CALL commonStream([${procSeedIds.join(",")}], [], ${radius},
              ${Neo4jEdgeDirection.BOTH}, ${options.procedurePageSize}, 1,
              '', false, null, 2, {}, 0, 0, 0, ${dbTimeout}, null)
          `,
        });
      }
    }

    if (selected.has("custom-name-search") && customSamples.names.length > 0) {
      const quotedNames = customSamples.names
        .map((n) => this.cypherString(n))
        .join(",");
      cases.push({
        key: "custom-name-search",
        label: "Custom: search by segment name",
        params: `count=${customSamples.names.length}; limit=${cypherLimit}`,
        responseType: DbResponseType.graph,
        isTimeboxed: false,
        renderGraph: true,
        query: `
          WITH [${quotedNames}] AS segmentNames
          MATCH (segment:SEGMENT)
          WHERE segment.segmentName IN segmentNames
          RETURN segment
          LIMIT ${cypherLimit}
        `,
      });
    }

    if (selected.has("custom-sequence-search")) {
      for (const sample of customSamples.sequenceSamples) {
        if (!sample.substring) {
          continue;
        }
        cases.push({
          key: "custom-sequence-search",
          label: "Custom: search by segment sequence",
          params: `length=${sample.length}; limit=${cypherLimit}`,
          responseType: DbResponseType.graph,
          isTimeboxed: false,
          renderGraph: true,
          query: `
            WITH [${this.cypherString(sample.substring)}] AS sequences
            MATCH (segment:SEGMENT)
            WHERE any(sequence IN sequences WHERE segment.segmentData CONTAINS sequence)
            OPTIONAL MATCH (segment)-[r]-(relatedSegment:SEGMENT)
            WHERE any(sequence IN sequences WHERE relatedSegment.segmentData CONTAINS sequence)
            RETURN DISTINCT segment, r, relatedSegment
            LIMIT ${cypherLimit}
          `,
        });
      }
    }

    if (selected.has("custom-sequence-chain")) {
      for (const chain of customSamples.chainSamples) {
        if (chain.sequences.length < 1) {
          continue;
        }
        const quotedSequences = chain.sequences
          .map((s) => this.cypherString(s))
          .join(",");
        cases.push({
          key: "custom-sequence-chain",
          label: "Custom: search by sequence chain",
          params: `maxJump=${chain.maxJumpLength}; minMatch=${chain.minSubsequenceMatchLength}; chainNodes=${chain.sequences.length}`,
          responseType: DbResponseType.table,
          isTimeboxed: false,
          renderGraph: true,
          toGraph: this.tableResponseToGraph.bind(this),
          query: `
            CALL sequenceChainSearch([${quotedSequences}], ${chain.maxJumpLength},
              ${chain.minSubsequenceMatchLength}, [], ${procedurePageSize}, 1, ${dbTimeout})
            YIELD nodes, nodeClass, nodeElementId, edges, edgeClass, edgeElementId,
              edgeSourceTargets, paths, indices
            RETURN nodes, nodeClass, nodeElementId, edges, edgeClass, edgeElementId,
              edgeSourceTargets, paths, indices
          `,
        });
      }
    }

    return cases;
  }

  private async resolveCustomQuerySamples(
    options: BenchmarkOptions,
  ): Promise<CustomQuerySamples> {
    const selected = new Set(options.selectedCases);
    const samples: CustomQuerySamples = {
      names: [],
      sequenceSamples: [],
      chainSamples: [],
    };

    if (selected.has("custom-name-search")) {
      samples.names = await this.sampleRandomNames(
        Math.max(1, options.customNameSampleCount),
      );
    }

    if (selected.has("custom-sequence-search")) {
      samples.sequenceSamples = await this.sampleSequenceSubstrings(
        options.customSequenceLengths,
      );
    }

    if (selected.has("custom-sequence-chain")) {
      samples.chainSamples = await this.sampleSequenceChains(
        Math.max(2, options.customChainNodeCount),
        options.customChainMaxJumpLengths,
        options.customChainMinSubsequenceMatchLength,
      );
    }

    return samples;
  }

  private async sampleRandomNames(count: number): Promise<string[]> {
    const response = (await this._db.runQueryPromised(
      `
        MATCH (s:SEGMENT)
        WITH s, rand() AS r
        ORDER BY r
        LIMIT ${count}
        RETURN s.segmentName AS segmentName
      `,
      DbResponseType.table,
      false,
    )) as TableResponse;
    const nameIndex = response.columns.indexOf("segmentName");
    if (nameIndex < 0) {
      return [];
    }
    return response.data
      .map((row) => row[nameIndex])
      .filter((x) => x !== undefined && x !== null)
      .map((x) => String(x));
  }

  private async sampleSequenceSubstrings(
    lengths: number[],
  ): Promise<CustomSequenceSample[]> {
    const samples: CustomSequenceSample[] = [];
    for (const length of lengths) {
      if (!Number.isFinite(length) || length < 1) {
        continue;
      }
      const response = (await this._db.runQueryPromised(
        `
          MATCH (s:SEGMENT)
          WHERE s.segmentLength >= ${length}
          WITH s, rand() AS r
          ORDER BY r
          LIMIT 1
          RETURN s.segmentName AS segmentName, s.segmentData AS segmentData
        `,
        DbResponseType.table,
        false,
      )) as TableResponse;
      const nameIndex = response.columns.indexOf("segmentName");
      const dataIndex = response.columns.indexOf("segmentData");
      if (response.data.length < 1 || dataIndex < 0) {
        continue;
      }
      const data = String(response.data[0][dataIndex] ?? "");
      if (data.length < length) {
        continue;
      }
      const maxStart = data.length - length;
      const start = Math.floor(Math.random() * (maxStart + 1));
      samples.push({
        length,
        substring: data.substring(start, start + length),
        sourceName:
          nameIndex >= 0 ? String(response.data[0][nameIndex] ?? "") : "",
      });
    }
    return samples;
  }

  private async sampleSequenceChains(
    nodeCount: number,
    maxJumpLengths: number[],
    minSubsequenceMatchLength: number,
  ): Promise<CustomChainSample[]> {
    const walk = await this.walkRandomChain(nodeCount);
    if (walk.datas.length < 1) {
      return [];
    }

    const chains: CustomChainSample[] = [];
    for (const rawJump of maxJumpLengths) {
      const maxJump = Math.max(0, Math.floor(rawJump));
      const sequences: string[] = [];
      const chainNodeNames: string[] = [];
      let i = 0;
      while (i < walk.datas.length) {
        if (walk.datas[i]) {
          sequences.push(walk.datas[i]);
          chainNodeNames.push(walk.names[i]);
        }
        const stride =
          maxJump > 0 ? 1 + Math.floor(Math.random() * (maxJump + 1)) : 1;
        i += stride;
      }
      if (sequences.length < 1) {
        continue;
      }
      chains.push({
        maxJumpLength: maxJump,
        minSubsequenceMatchLength: Math.max(0, minSubsequenceMatchLength),
        sequences,
        chainNodeNames,
      });
    }
    return chains;
  }

  private async walkRandomChain(
    nodeCount: number,
  ): Promise<{ ids: string[]; names: string[]; datas: string[] }> {
    const result = { ids: [] as string[], names: [] as string[], datas: [] as string[] };
    const startResp = (await this._db.runQueryPromised(
      `
        MATCH (s:SEGMENT)
        WITH s, rand() AS r
        ORDER BY r
        LIMIT 1
        RETURN elementId(s) AS id, s.segmentName AS segmentName, s.segmentData AS segmentData
      `,
      DbResponseType.table,
      false,
    )) as TableResponse;
    if (!startResp.data || startResp.data.length < 1) {
      return result;
    }
    const idIndex = startResp.columns.indexOf("id");
    const nameIndex = startResp.columns.indexOf("segmentName");
    const dataIndex = startResp.columns.indexOf("segmentData");
    result.ids.push(String(startResp.data[0][idIndex]));
    result.names.push(String(startResp.data[0][nameIndex] ?? ""));
    result.datas.push(String(startResp.data[0][dataIndex] ?? ""));

    const visited = new Set<string>(result.ids);
    for (let i = 1; i < nodeCount; i++) {
      const currentId = result.ids[result.ids.length - 1];
      const stepResp = (await this._db.runQueryPromised(
        `
          MATCH (s:SEGMENT)-[]-(n:SEGMENT)
          WHERE elementId(s) = ${this.cypherString(currentId)}
          WITH n, rand() AS r
          ORDER BY r
          LIMIT 10
          RETURN elementId(n) AS id, n.segmentName AS segmentName, n.segmentData AS segmentData
        `,
        DbResponseType.table,
        false,
      )) as TableResponse;
      if (!stepResp.data || stepResp.data.length < 1) {
        break;
      }
      const stepIdIndex = stepResp.columns.indexOf("id");
      const stepNameIndex = stepResp.columns.indexOf("segmentName");
      const stepDataIndex = stepResp.columns.indexOf("segmentData");
      let next: { id: string; name: string; data: string } | undefined;
      for (const row of stepResp.data) {
        const id = String(row[stepIdIndex]);
        if (!visited.has(id)) {
          next = {
            id,
            name: String(row[stepNameIndex] ?? ""),
            data: String(row[stepDataIndex] ?? ""),
          };
          break;
        }
      }
      if (!next) {
        const row = stepResp.data[0];
        next = {
          id: String(row[stepIdIndex]),
          name: String(row[stepNameIndex] ?? ""),
          data: String(row[stepDataIndex] ?? ""),
        };
      }
      visited.add(next.id);
      result.ids.push(next.id);
      result.names.push(next.name);
      result.datas.push(next.data);
    }
    return result;
  }

  private async resolveProcedureSeeds(
    options: BenchmarkOptions,
    globalSeeds: BenchmarkSeed[],
  ): Promise<BenchmarkSeed[] | null> {
    if (!options.useProcedurePool) {
      return null;
    }
    const selected = new Set(options.selectedCases);
    if (
      !selected.has("pg2-graph-of-interest") &&
      !selected.has("pg2-common-stream")
    ) {
      return null;
    }
    if (globalSeeds.length < 1) {
      return null;
    }

    const MAX_ANCHORS = 5;
    const POOL_PER_ANCHOR_LIMIT = 5000;
    const radius = Math.max(1, Math.floor(options.procedurePoolRadius));
    const threshold = Math.max(1, Math.floor(options.procedurePoolMinNodes));
    const seedCount = Math.max(2, Math.floor(options.seedCount));

    const anchors = await this.fetchCandidateAnchors(options, MAX_ANCHORS);
    if (anchors.length < 1) {
      return null;
    }
    const anchorIds = new Set(anchors.map((a) => a.id));

    const pool = new Map<string, ProcedurePoolNode>();
    for (const anchor of anchors) {
      const neighbors = await this.fetchAnchorNeighborhood(
        anchor.id,
        radius,
        POOL_PER_ANCHOR_LIMIT,
      );
      for (const n of neighbors) {
        if (anchorIds.has(n.id)) {
          continue;
        }
        const existing = pool.get(n.id);
        if (!existing || n.distance > existing.distance) {
          pool.set(n.id, n);
        }
      }
      if (pool.size >= threshold) {
        break;
      }
    }

    if (pool.size < 1) {
      return null;
    }

    const sorted = Array.from(pool.values()).sort(
      (a, b) => b.distance - a.distance,
    );
    return sorted.slice(0, seedCount).map((n) => ({
      id: n.id,
      segmentName: n.segmentName,
      degree: undefined,
    }));
  }

  private async fetchCandidateAnchors(
    options: BenchmarkOptions,
    maxAnchors: number,
  ): Promise<BenchmarkSeed[]> {
    const userNames = options.seedSegmentNames.filter((x) => x.length > 0);
    let query: string;
    if (userNames.length > 0) {
      query = `
        MATCH (s:SEGMENT)
        WHERE s.segmentName IN [${userNames
          .map((x) => this.cypherString(x))
          .join(",")}]
        OPTIONAL MATCH (s)-[r]-()
        WITH s, count(r) AS degree
        RETURN elementId(s) AS id, s.segmentName AS segmentName, degree
        ORDER BY degree DESC
        LIMIT ${maxAnchors}
      `;
    } else {
      query = `
        MATCH (s:SEGMENT)-[r]-()
        WITH s, count(r) AS degree
        RETURN elementId(s) AS id, s.segmentName AS segmentName, degree
        ORDER BY degree DESC
        LIMIT ${maxAnchors}
      `;
    }
    const response = (await this._db.runQueryPromised(
      query,
      DbResponseType.table,
      false,
    )) as TableResponse;
    return this.tableRowsToSeeds(response);
  }

  private async fetchAnchorNeighborhood(
    anchorId: string,
    radius: number,
    limit: number,
  ): Promise<ProcedurePoolNode[]> {
    const response = (await this._db.runQueryPromised(
      `
        MATCH (anchor:SEGMENT) WHERE elementId(anchor) = ${this.cypherString(anchorId)}
        MATCH p = shortestPath((anchor)-[*1..${radius}]-(neighbor:SEGMENT))
        WHERE anchor <> neighbor
        RETURN elementId(neighbor) AS id,
               neighbor.segmentName AS segmentName,
               length(p) AS distance
        ORDER BY distance DESC, neighbor.segmentName ASC
        LIMIT ${limit}
      `,
      DbResponseType.table,
      false,
    )) as TableResponse;
    const idIdx = response.columns.indexOf("id");
    const nameIdx = response.columns.indexOf("segmentName");
    const distIdx = response.columns.indexOf("distance");
    if (idIdx < 0 || distIdx < 0) {
      return [];
    }
    return response.data.map((row) => ({
      id: String(row[idIdx]),
      segmentName: nameIdx >= 0 ? String(row[nameIdx] ?? "") : "",
      distance: Number(row[distIdx] ?? 0),
    }));
  }

  private async resolveSeeds(options: BenchmarkOptions): Promise<BenchmarkSeed[]> {
    const seedCount = Math.max(1, options.seedCount);
    const seedNames = options.seedSegmentNames.filter((x) => x.length > 0);
    let query: string;

    if (seedNames.length > 0) {
      query = `
        MATCH (s:SEGMENT)
        WHERE s.segmentName IN [${seedNames
          .map((x) => this.cypherString(x))
          .join(",")}]
        OPTIONAL MATCH (s)-[r]-()
        WITH s, count(r) AS degree
        RETURN elementId(s) AS id, s.segmentName AS segmentName, degree
        ORDER BY degree DESC
        LIMIT ${seedCount}
      `;
    } else {
      query = `
        MATCH (s:SEGMENT)-[r]-()
        WITH s, count(r) AS degree
        RETURN elementId(s) AS id, s.segmentName AS segmentName, degree
        ORDER BY degree DESC
        LIMIT ${seedCount}
      `;
    }

    const response = (await this._db.runQueryPromised(
      query,
      DbResponseType.table,
      false,
    )) as TableResponse;
    let seeds = this.tableRowsToSeeds(response);

    if (seeds.length < seedCount && seedNames.length === 0) {
      const fallback = (await this._db.runQueryPromised(
        `
          MATCH (s:SEGMENT)
          RETURN elementId(s) AS id, s.segmentName AS segmentName, 0 AS degree
          LIMIT ${seedCount}
        `,
        DbResponseType.table,
        false,
      )) as TableResponse;
      seeds = this.tableRowsToSeeds(fallback);
    }

    if (seeds.length < 1) {
      throw new Error("No SEGMENT seed could be found for benchmark queries.");
    }

    return seeds;
  }

  private tableRowsToSeeds(response: TableResponse): BenchmarkSeed[] {
    const idIndex = response.columns.indexOf("id");
    const nameIndex = response.columns.indexOf("segmentName");
    const degreeIndex = response.columns.indexOf("degree");
    return response.data.map((row) => ({
      id: String(row[idIndex]),
      segmentName: String(row[nameIndex] ?? ""),
      degree:
        degreeIndex > -1 && row[degreeIndex] !== undefined
          ? Number(row[degreeIndex])
          : undefined,
    }));
  }

  private tableResponseToGraph(data: TableResponse): GraphResponse {
    const indexNodes = data.columns.indexOf("nodes");
    const indexNodeId = data.columns.indexOf("nodeElementId");
    const indexNodeClass = data.columns.indexOf("nodeClass");
    const indexEdges = data.columns.indexOf("edges");
    const indexEdgeId = data.columns.indexOf("edgeElementId");
    const indexEdgeClass = data.columns.indexOf("edgeClass");
    const indexEdgeSourceTarget = data.columns.indexOf("edgeSourceTargets");
    const row = data.data[0] || [];

    const nodes = row[indexNodes] || [];
    const nodeClass = row[indexNodeClass] || [];
    const nodeId = row[indexNodeId] || [];
    const edges = row[indexEdges] || [];
    const edgeClass = row[indexEdgeClass] || [];
    const edgeId = row[indexEdgeId] || [];
    const edgeSourceTarget = row[indexEdgeSourceTarget] || [];

    const graph: GraphResponse = { nodes: [], edges: [] };
    const nodeIds: Record<string, boolean> = {};

    for (let i = 0; i < nodes.length; i++) {
      graph.nodes.push({
        elementId: String(nodeId[i]),
        labels: [String(nodeClass[i])],
        properties: nodes[i],
      });
      nodeIds[String(nodeId[i])] = true;
    }

    for (let i = 0; i < edges.length; i++) {
      const sourceTarget = edgeSourceTarget[i] || [];
      const sourceId = String(sourceTarget[0]);
      const targetId = String(sourceTarget[1]);
      if (nodeIds[sourceId] && nodeIds[targetId]) {
        graph.edges.push({
          properties: edges[i],
          startNodeElementId: sourceId,
          endNodeElementId: targetId,
          elementId: String(edgeId[i]),
          type: String(edgeClass[i]),
        });
      }
    }

    return graph;
  }

  private async renderGraph(
    graph: GraphResponse,
    options: BenchmarkOptions,
  ): Promise<RenderTiming> {
    const renderStart = performance.now();
    let loadEnd = renderStart;
    let layoutStart: number | undefined;
    let layoutStop: number | undefined;

    const layoutPromise = new Promise<RenderTiming>((resolve) => {
      const timeoutId = setTimeout(() => {
        cleanup();
        resolve({
          renderSetupMs: loadEnd - renderStart,
          layoutQueueMs:
            layoutStart !== undefined ? layoutStart - loadEnd : undefined,
          layoutMs:
            layoutStart !== undefined && layoutStop !== undefined
              ? layoutStop - layoutStart
              : undefined,
          totalMs: performance.now() - renderStart,
          layoutTimedOut: true,
        });
      }, options.layoutTimeoutMs);

      const onLayoutStart = () => {
        layoutStart = performance.now();
      };
      const onLayoutStop = () => {
        layoutStop = performance.now();
        cleanup();
        this.afterPaint().then(() => {
          resolve({
            renderSetupMs: loadEnd - renderStart,
            layoutQueueMs:
              layoutStart !== undefined ? layoutStart - loadEnd : undefined,
            layoutMs:
              layoutStart !== undefined && layoutStop !== undefined
                ? layoutStop - layoutStart
                : undefined,
            totalMs: performance.now() - renderStart,
            layoutTimedOut: false,
          });
        });
      };
      const cleanup = () => {
        clearTimeout(timeoutId);
        this._g.cy.off("layoutstart", onLayoutStart);
        this._g.cy.off("layoutstop", onLayoutStop);
      };

      this._g.cy.one("layoutstart", onLayoutStart);
      this._g.cy.one("layoutstop", onLayoutStop);
    });

    this._cyService.loadElementsFromDatabase(
      graph,
      false,
      false,
      true,
    );
    loadEnd = performance.now();

    return layoutPromise;
  }

  private afterPaint(): Promise<void> {
    return new Promise((resolve) => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => resolve());
      });
    });
  }

  private getTableRowCount(response: TableResponse): number {
    return response && response.data ? response.data.length : 0;
  }

  private payloadKb(response: unknown): number {
    try {
      return this.round(JSON.stringify(response).length / 1024, 2);
    } catch {
      return 0;
    }
  }

  private browserHeapMb(): number | undefined {
    const memory = (performance as any).memory;
    if (!memory || memory.usedJSHeapSize === undefined) {
      return undefined;
    }
    return this.round(memory.usedJSHeapSize / (1024 * 1024), 2);
  }

  private csvCell(value: unknown): string {
    if (value === undefined || value === null) {
      return "";
    }
    const str = String(value);
    if (str.includes(",") || str.includes("\n") || str.includes("\"")) {
      return `"${str.replace(/"/g, "\"\"")}"`;
    }
    return str;
  }

  private cypherString(value: string): string {
    return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }

  private round(value: number, digits: number): number {
    const m = Math.pow(10, digits);
    return Math.round(value * m) / m;
  }

  private errorMessage(e: unknown): string {
    if (e instanceof Error) {
      return e.message;
    }
    if (typeof e === "string") {
      return e;
    }
    try {
      return JSON.stringify(e);
    } catch {
      return "Unknown benchmark error";
    }
  }
}
