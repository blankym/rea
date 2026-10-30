import { artifactExpansionBudgetBytes } from "../domain/artifactExpansionBudget.js";
import type { FileHandle } from "node:fs/promises";
import { setImmediate } from "node:timers/promises";
import { Readable, Transform, pipeline } from "node:stream";
import {
  ArtifactReaderFailure,
  copyArtifactEntry,
  sameArtifactEntry,
  type ArtifactEntry,
  type ArtifactReader,
  type ArtifactTraversalOptions,
} from "./ArtifactReader.js";
import { ArtifactResourceScope } from "./ArtifactResourceScope.js";
import { assertArtifactReadActive } from "./ArtifactFileOperation.js";
import { readPckBytes } from "./GodotPckLocation.js";
import type { UnityFsMetadata } from "../domain/artifactFormatMetadata.js";
import { jsonValueByteLength } from "../domain/jsonValueByteLength.js";
import type { StableRegularFileDescriptor } from "../filesystem/RegularFile.js";
import { ArtifactFileSource } from "./ArtifactFileSource.js";
import { decodeLz4Block } from "../gpu/Lz4Block.js";
import {
  UNITYFS_COMPRESSION_LZ4,
  UNITYFS_COMPRESSION_LZ4HC,
  UNITYFS_COMPRESSION_LZMA,
  UNITYFS_COMPRESSION_NONE,
  UNITYFS_FLAG_COMPRESSION_MASK,
  UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION,
} from "../domain/unityfsFormat.js";
import {
  readUnityFsBlocksInfo,
  readUnityFsHeader,
  unityFsFormatFailure,
  type UnityFsBlock,
  type UnityFsNode,
} from "./UnityFsStructures.js";

/** Encoded retained entry metadata, including escaped paths and source facts. */
const MAX_DIRECTORY_METADATA_BYTES = 64 * 1024 * 1024;

const directoryStatus = (flags: number): UnityFsMetadata["directory_status"] =>
  (flags & UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION) !== 0
    ? "encrypted"
    : (flags & UNITYFS_FLAG_COMPRESSION_MASK) === UNITYFS_COMPRESSION_LZMA
      ? "unsupported-compression"
      : "available";

const metadataLimit = (): ArtifactReaderFailure =>
  new ArtifactReaderFailure(
    "limit",
    "UnityFS directory exceeds the 64 MiB encoded metadata budget",
    { boundary: "memory" },
  );

/** Whether every block covering the member decodes; undefined means readable. */
type UnityFsAvailability = (node: UnityFsNode) => string | undefined;

/** One descriptor, one validated directory, and bounded per-block member reads. */
export class UnityFsArtifactReader implements ArtifactReader {
  readonly format = "unityfs" as const;
  readonly #scope = new ArtifactResourceScope();
  readonly #entries = new Map<string, ArtifactEntry>();
  readonly #nodes = new Map<string, UnityFsNode>();
  readonly #streams = new Set<Readable>();
  readonly #source: ArtifactFileSource;
  #prepared: Promise<void> | undefined;
  #header: UnityFsMetadata | undefined;
  #blocks: readonly UnityFsBlock[] = [];
  #dataStart = 0;
  #unavailableCount = 0;
  #closing = false;

  constructor(path: string, admitted?: StableRegularFileDescriptor) {
    this.#source = new ArtifactFileSource(path, "UnityFS bundle", admitted);
  }

  formatMetadata(): UnityFsMetadata | undefined {
    return this.#header === undefined ? undefined : { ...this.#header };
  }

  async prepareContainer(
    expectedSha256: string,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.#scope.run(async () => {
      this.#assertOpen(signal);
      await this.#source.bindSha256(expectedSha256, signal);
    });
  }

  async *entries(
    signal?: AbortSignal,
    options?: ArtifactTraversalOptions,
  ): AsyncIterable<ArtifactEntry> {
    await this.#scope.run(async () => {
      this.#assertOpen(signal);
      this.#prepared ??= this.#prepare(signal);
      await this.#prepared;
      await this.#source.read(signal);
    });
    if (this.#header?.directory_status !== "available") {
      options?.onCoverage?.({
        status: "unknown",
        limitations: [
          this.#header?.directory_status === "encrypted"
            ? "UnityFS bundle declares asset-bundle encryption; member identities and contents remain unknown."
            : "UnityFS blocks-and-directory table uses an unsupported compression; member identities remain unknown.",
        ],
      });
      return;
    }
    let index = 0;
    for (const entry of this.#entries.values()) {
      if (index++ % 256 === 0) await setImmediate();
      this.#assertOpen(signal);
      yield copyArtifactEntry(entry);
    }
    if (this.#unavailableCount > 0)
      options?.onCoverage?.({
        status: "unknown",
        limitations: [
          `${String(this.#unavailableCount)} UnityFS member(s) use unsupported block compression and were listed without content hashes.`,
        ],
      });
  }

  async open(entry: ArtifactEntry, signal?: AbortSignal): Promise<Readable> {
    return this.#scope.run(async () => {
      this.#assertOpen(signal);
      const stored = this.#entries.get(entry.adapterKey);
      const node = this.#nodes.get(entry.adapterKey);
      if (
        stored === undefined ||
        node === undefined ||
        !sameArtifactEntry(entry, stored)
      )
        throw new ArtifactReaderFailure(
          "integrity",
          "UnityFS entry does not match its owning inventory",
        );
      if (stored.unavailable !== undefined)
        throw new ArtifactReaderFailure("unavailable", stored.unavailable);
      const { handle } = await this.#source.read(signal);
      this.#assertOpen(signal);
      const source = this.#memberSource(handle, stored, node, signal);
      const output = this.#memberStream(stored, signal);
      // The returned stream still reports failures to its consumer. Also handle
      // retirement before a caller has attached its own error listener.
      output.on("error", () => undefined);
      this.#streams.add(output);
      // Writable completion can leave an unread member buffered on the readable
      // side. The returned stream stays owned until that side closes too.
      output.once("close", () => this.#streams.delete(output));
      void this.#scope
        .run(
          () =>
            new Promise<void>((resolve) => {
              pipeline(source, output, () => {
                resolve();
              });
            }),
        )
        .catch((cause: unknown) =>
          output.destroy(
            cause instanceof Error
              ? cause
              : new Error("UnityFS stream ownership failed", { cause }),
          ),
        );
      return output;
    });
  }

  async close(): Promise<void> {
    this.#closing = true;
    for (const stream of this.#streams)
      stream.destroy(
        new ArtifactReaderFailure("unavailable", "UnityFS reader closed"),
      );
    await this.#scope.close();
    await this.#source.close();
    this.#entries.clear();
    this.#nodes.clear();
  }

  provenance(): readonly [] {
    return [];
  }

  #assertOpen(signal?: AbortSignal): void {
    assertArtifactReadActive(signal);
    if (this.#closing)
      throw new ArtifactReaderFailure(
        "unavailable",
        "UnityFS reader is closed",
      );
  }

  async #prepare(signal?: AbortSignal): Promise<void> {
    const { handle, initial } = await this.#source.read(signal);
    const header = await readUnityFsHeader(handle, initial.size, signal);
    if (header === undefined)
      throw unityFsFormatFailure("no UnityFS signature or header");
    const status = directoryStatus(header.flags);
    this.#header = {
      kind: "unityfs",
      signature: "UnityFS",
      format_version: header.formatVersion,
      player_version: header.playerVersion,
      engine_version: header.engineVersion,
      declared_size: header.declaredSize,
      flags: header.flags,
      blocks_info_compressed_size: header.compressedBlocksInfoSize,
      blocks_info_uncompressed_size: header.uncompressedBlocksInfoSize,
      blocks_info_at_end: header.blocksInfoAtEnd,
      data_start: 0,
      block_count: 0,
      data_hash: null,
      directory_status: status,
    };
    if (status !== "available") return;
    const { dataHash, blocks, nodes, dataStart } = await readUnityFsBlocksInfo(
      handle,
      header,
      initial.size,
      signal,
    );
    this.#assertBlocksWithinBudget(blocks, initial.size);
    this.#header = {
      ...this.#header,
      data_hash: dataHash,
      block_count: blocks.length,
      data_start: dataStart,
    };
    this.#dataStart = dataStart;
    this.#blocks = blocks;
    await this.#registerNodes(
      nodes,
      BigInt(artifactExpansionBudgetBytes(initial.size)),
      unityFsMemberAvailability(blocks),
      signal,
    );
    await this.#source.read(signal);
  }

  #assertBlocksWithinBudget(
    blocks: readonly UnityFsBlock[],
    fileSize: number,
  ): void {
    const expansionBudget = BigInt(artifactExpansionBudgetBytes(fileSize));
    let declaredStreamBytes = 0n;
    for (const block of blocks) {
      declaredStreamBytes += BigInt(block.uncompressedSize);
      if (declaredStreamBytes > expansionBudget)
        throw new ArtifactReaderFailure(
          "limit",
          `UnityFS blocks declare ${String(declaredStreamBytes)} stream bytes, exceeding this bundle's ${String(expansionBudget)}-byte expansion budget`,
        );
    }
  }

  async #registerNodes(
    nodes: readonly UnityFsNode[],
    expansionBudget: bigint,
    availability: UnityFsAvailability,
    signal?: AbortSignal,
  ): Promise<void> {
    let retainedBytes = 0;
    let declaredReadableBytes = 0n;
    for (let index = 0; index < nodes.length; index++) {
      if (index % 256 === 0) await setImmediate();
      assertArtifactReadActive(signal);
      const node = nodes[index]!;
      const adapterKey = `unityfs-entry:${String(index)}`;
      const unavailable = availability(node);
      const encrypted = (node.flags & 1) !== 0;
      // Aliases and repeated ranges are valid, but each path causes another
      // read. Charge every readable member, including overlapping ranges.
      if (unavailable === undefined && !encrypted) {
        declaredReadableBytes += BigInt(node.size);
        if (declaredReadableBytes > expansionBudget)
          throw new ArtifactReaderFailure(
            "limit",
            `UnityFS readable members declare ${String(declaredReadableBytes)} bytes, exceeding this bundle's ${String(expansionBudget)}-byte expansion budget`,
            { boundary: "memory" },
          );
      }
      const metadata = {
        path: node.path,
        sourcePath: node.path,
        kind: "file" as const,
        declaredSize: node.size,
        compressedSize: null,
        executable: false,
        encrypted,
        byteOffset: node.offset,
        declaredIntegrity: null,
        unpacked: false,
        ...(unavailable === undefined ? {} : { unavailable }),
        limitations: unavailable === undefined ? [] : [unavailable],
        adapterKey,
        formatMetadata: {
          kind: "unityfs-entry" as const,
          index,
          source_path: node.path,
          declared_offset: node.offset,
          flags: node.flags,
        },
      } satisfies ArtifactEntry;
      retainedBytes += jsonValueByteLength(
        metadata,
        MAX_DIRECTORY_METADATA_BYTES - retainedBytes,
      );
      if (retainedBytes > MAX_DIRECTORY_METADATA_BYTES) throw metadataLimit();
      if (unavailable !== undefined) this.#unavailableCount += 1;
      this.#entries.set(adapterKey, metadata);
      this.#nodes.set(adapterKey, node);
    }
  }

  /** Decompress only the blocks covering the member range, one at a time. */
  #memberSource(
    handle: FileHandle,
    entry: ArtifactEntry,
    node: UnityFsNode,
    signal?: AbortSignal,
  ): Readable {
    const blocks = this.#blocks;
    const dataStart = this.#dataStart;
    const end = node.offset + node.size;
    const first = blocks.findIndex(
      (block) => block.streamStart + block.uncompressedSize > node.offset,
    );
    const last =
      node.size === 0
        ? first
        : blocks.findIndex(
            (block) => block.streamStart + block.uncompressedSize >= end,
          );
    if (node.size !== 0 && (first === -1 || last === -1))
      throw unityFsFormatFailure(
        `member range is beyond the stream: ${entry.path}`,
      );
    return Readable.from(
      (async function* () {
        try {
          if (node.size === 0) return;
          for (let index = first; index <= last; index++) {
            assertArtifactReadActive(signal);
            const block = blocks[index]!;
            const compressed = await readPckBytes(
              handle,
              dataStart + block.compressedStart,
              block.compressedSize,
              dataStart +
                (blocks[index + 1]?.compressedStart ??
                  block.compressedStart + block.compressedSize),
              signal,
            );
            const plain = await decodeUnityFsBlock(block, compressed, signal);
            const sliceStart = Math.max(node.offset - block.streamStart, 0);
            const sliceEnd = Math.min(
              end - block.streamStart,
              block.uncompressedSize,
            );
            if (sliceEnd > sliceStart)
              yield plain.subarray(sliceStart, sliceEnd);
            if (index % 64 === 63) await setImmediate();
          }
        } catch (cause: unknown) {
          if (cause instanceof ArtifactReaderFailure) throw cause;
          throw new ArtifactReaderFailure(
            "format",
            `UnityFS member read failed: ${entry.path}`,
            { cause },
          );
        }
      })(),
    );
  }

  /** Retire a member stream only after its delivered bytes verify complete. */
  #memberStream(entry: ArtifactEntry, signal?: AbortSignal): Transform {
    let bytes = 0;
    const check = async (): Promise<void> => {
      if (bytes !== entry.declaredSize)
        throw unityFsFormatFailure(`member ended early: ${entry.path}`);
      await this.#source.read(signal);
    };
    return new Transform({
      transform(chunk: Buffer, _encoding, callback) {
        bytes += chunk.length;
        callback(null, chunk);
      },
      flush(callback) {
        void check().then(
          () => callback(),
          (cause: unknown) =>
            callback(
              cause instanceof Error
                ? cause
                : new Error("UnityFS read failed", { cause }),
            ),
        );
      },
    });
  }
}

const SUPPORTED_MEMBER_COMPRESSIONS = new Set([
  UNITYFS_COMPRESSION_NONE,
  UNITYFS_COMPRESSION_LZ4,
  UNITYFS_COMPRESSION_LZ4HC,
]);

/**
 * Report a member's unsupported-covering-block reason, or undefined when every
 * covering block decodes. Blocks are stream-ordered, so a member's covering
 * range is contiguous and a prefix count keeps the check O(log n) per member.
 */
const unityFsMemberAvailability = (
  blocks: readonly UnityFsBlock[],
): UnityFsAvailability => {
  const unsupportedPrefix: number[] = [0];
  for (const block of blocks)
    unsupportedPrefix.push(
      unsupportedPrefix[unsupportedPrefix.length - 1]! +
        (SUPPORTED_MEMBER_COMPRESSIONS.has(
          block.flags & UNITYFS_FLAG_COMPRESSION_MASK,
        )
          ? 0
          : 1),
    );
  const covering = (
    offset: number,
    end: number,
  ): { readonly first: number; readonly last: number } | undefined => {
    let lo = 0;
    let hi = blocks.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const block = blocks[mid]!;
      if (block.streamStart + block.uncompressedSize > offset) hi = mid;
      else lo = mid + 1;
    }
    const first = lo;
    hi = blocks.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (blocks[mid]!.streamStart < end) lo = mid + 1;
      else hi = mid;
    }
    const last = lo - 1;
    return first < blocks.length && last >= first ? { first, last } : undefined;
  };
  return (node: UnityFsNode): string | undefined => {
    if (node.size === 0) return undefined;
    const range = covering(node.offset, node.offset + node.size);
    if (range === undefined)
      return "UnityFS member range has no covering blocks";
    if (
      unsupportedPrefix[range.last + 1]! - unsupportedPrefix[range.first]! ===
      0
    )
      return undefined;
    const mask = blocks
      .slice(range.first, range.last + 1)
      .map((block) => block.flags & UNITYFS_FLAG_COMPRESSION_MASK)
      .find((value) => !SUPPORTED_MEMBER_COMPRESSIONS.has(value))!;
    return mask === UNITYFS_COMPRESSION_LZMA
      ? "UnityFS member uses unsupported LZMA compression"
      : `UnityFS member uses unsupported block compression 0x${String(mask)}`;
  };
};

const decodeUnityFsBlock = async (
  block: UnityFsBlock,
  compressed: Buffer,
  signal?: AbortSignal,
): Promise<Buffer> => {
  switch (block.flags & UNITYFS_FLAG_COMPRESSION_MASK) {
    case UNITYFS_COMPRESSION_NONE:
      if (compressed.length !== block.uncompressedSize)
        throw unityFsFormatFailure(
          "plain block size disagrees with its declaration",
        );
      return compressed;
    case UNITYFS_COMPRESSION_LZ4:
    case UNITYFS_COMPRESSION_LZ4HC:
      try {
        return await decodeLz4Block(compressed, block.uncompressedSize, signal);
      } catch (cause: unknown) {
        if (cause instanceof ArtifactReaderFailure) throw cause;
        throw new ArtifactReaderFailure(
          "format",
          "UnityFS is malformed: LZ4 block does not decode to its declared size",
          { cause },
        );
      }
    case UNITYFS_COMPRESSION_LZMA:
      throw new ArtifactReaderFailure(
        "unavailable",
        "UnityFS LZMA-compressed blocks are unsupported",
      );
    default:
      throw new ArtifactReaderFailure(
        "unavailable",
        `UnityFS block compression 0x${String(block.flags & UNITYFS_FLAG_COMPRESSION_MASK)} is unsupported`,
      );
  }
};
