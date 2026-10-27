import type { FileHandle } from "node:fs/promises";
import { ArtifactReaderFailure } from "./ArtifactReader.js";
import { readPckBytes } from "./GodotPckLocation.js";
import { decodeLz4Block } from "../gpu/Lz4Block.js";
import {
  readUnityFsHeaderFacts,
  UNITYFS_COMPRESSION_LZ4,
  UNITYFS_COMPRESSION_LZ4HC,
  UNITYFS_COMPRESSION_NONE,
  UNITYFS_FLAG_BLOCKS_INFO_AT_END,
  UNITYFS_FLAG_BLOCK_INFO_NEEDS_PADDING,
  UNITYFS_FLAG_COMPRESSION_MASK,
  UNITYFS_FORMAT_VERSION_MAX,
  UNITYFS_FORMAT_VERSION_MIN,
  UNITYFS_HEADER_STRING_MAX,
  UNITYFS_SIGNATURE,
  unityfsHeaderAligned,
} from "../domain/unityfsFormat.js";

/** One UnityFS block is producer-chunked well below this; larger claims are malformed. */
const MAX_BLOCK_UNCOMPRESSED_BYTES = 64 * 1024 * 1024;
/** Signature, version, two bounded strings and the fixed tail; reads are bounded. */
const HEADER_PREFIX_MAX = 8 + 4 + UNITYFS_HEADER_STRING_MAX * 2 + 20;
const UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export interface UnityFsHeader {
  readonly formatVersion: number;
  readonly playerVersion: string;
  readonly engineVersion: string;
  readonly declaredSize: number;
  readonly flags: number;
  readonly compressedBlocksInfoSize: number;
  readonly uncompressedBlocksInfoSize: number;
  readonly blocksInfoOffset: number;
  readonly blocksInfoAtEnd: boolean;
  /** Aligned position right after the header; the data area start when blocks-info trails. */
  readonly afterHeader: number;
  /** Whether format version and producer revision use 16-byte block alignment. */
  readonly usesBlockAlignment: boolean;
}

export interface UnityFsBlock {
  readonly uncompressedSize: number;
  readonly compressedSize: number;
  readonly flags: number;
  /** Cumulative compressed byte offset inside the data area. */
  readonly compressedStart: number;
  /** Cumulative uncompressed byte offset inside the virtual stream. */
  readonly streamStart: number;
}

export interface UnityFsNode {
  readonly offset: number;
  readonly size: number;
  readonly flags: number;
  readonly path: string;
}

export interface UnityFsBlocksInfo {
  readonly dataHash: string;
  readonly blocks: readonly UnityFsBlock[];
  readonly nodes: readonly UnityFsNode[];
  readonly dataStart: number;
}

export const unityFsFormatFailure = (detail: string): ArtifactReaderFailure =>
  new ArtifactReaderFailure("format", `UnityFS is malformed: ${detail}`);

const align16 = (value: number): number => (value + 15) & ~15;

/** Decode header fields once; unsupported content does not erase observed headers. */
export const readUnityFsHeader = async (
  handle: FileHandle,
  size: number,
  signal?: AbortSignal,
): Promise<UnityFsHeader | undefined> => {
  const prefix = await readPckBytes(
    handle,
    0,
    Math.min(size, HEADER_PREFIX_MAX),
    size,
    signal,
  );
  if (prefix.length < 20 || !prefix.subarray(0, 8).equals(UNITYFS_SIGNATURE))
    return undefined;
  const facts = readUnityFsHeaderFacts(prefix);
  if (facts === undefined)
    throw unityFsFormatFailure("header is truncated or exceeds its bounds");
  if (
    facts.formatVersion < UNITYFS_FORMAT_VERSION_MIN ||
    facts.formatVersion > UNITYFS_FORMAT_VERSION_MAX
  )
    throw new ArtifactReaderFailure(
      "unavailable",
      `UnityFS format ${String(facts.formatVersion)} is unsupported; ${String(UNITYFS_FORMAT_VERSION_MIN)}-${String(UNITYFS_FORMAT_VERSION_MAX)} are supported`,
    );
  if (facts.declaredSize > BigInt(size))
    throw unityFsFormatFailure("declared size exceeds the file");
  const [engineMajor, engineMinor, enginePatch] = facts.engineVersion
    .split(".")
    .map((part) => Number.parseInt(part, 10));
  const usesBlockAlignment = unityfsHeaderAligned(
    facts.formatVersion,
    Number.isNaN(engineMajor) ? undefined : engineMajor,
    Number.isNaN(engineMinor) ? undefined : engineMinor,
    Number.isNaN(enginePatch) ? undefined : enginePatch,
  );
  const position = usesBlockAlignment
    ? align16(facts.afterFixed)
    : facts.afterFixed;
  const blocksInfoAtEnd = (facts.flags & UNITYFS_FLAG_BLOCKS_INFO_AT_END) !== 0;
  if (blocksInfoAtEnd && facts.compressedBlocksInfoSize > size)
    throw unityFsFormatFailure("declared blocks-info exceeds the file");
  return {
    formatVersion: facts.formatVersion,
    playerVersion: facts.playerVersion,
    engineVersion: facts.engineVersion,
    declaredSize: Number(facts.declaredSize),
    flags: facts.flags,
    compressedBlocksInfoSize: facts.compressedBlocksInfoSize,
    uncompressedBlocksInfoSize: facts.uncompressedBlocksInfoSize,
    blocksInfoAtEnd,
    blocksInfoOffset: blocksInfoAtEnd
      ? size - facts.compressedBlocksInfoSize
      : position,
    afterHeader: position,
    usesBlockAlignment,
  };
};

const decodeBlocksInfoTable = async (
  header: UnityFsHeader,
  compressed: Buffer,
  signal?: AbortSignal,
): Promise<Buffer> => {
  const compression = header.flags & UNITYFS_FLAG_COMPRESSION_MASK;
  if (compression === UNITYFS_COMPRESSION_NONE) return compressed;
  if (
    compression === UNITYFS_COMPRESSION_LZ4 ||
    compression === UNITYFS_COMPRESSION_LZ4HC
  )
    return decodeLz4Block(
      compressed,
      header.uncompressedBlocksInfoSize,
      signal,
    );
  throw new ArtifactReaderFailure(
    "unavailable",
    `UnityFS blocks-info compression 0x${String(compression)} is unsupported`,
  );
};

const parseBlocks = (table: Buffer): UnityFsBlock[] => {
  if (table.length < 24)
    throw unityFsFormatFailure("blocks-info table is truncated");
  const blockCount = table.readUInt32BE(16);
  if (20 + blockCount * 10 + 4 > table.length)
    throw unityFsFormatFailure("declared block count exceeds the table");
  const blocks: UnityFsBlock[] = [];
  let cursor = 20;
  let compressedStart = 0;
  let streamStart = 0;
  for (let index = 0; index < blockCount; index++) {
    const uncompressedSize = table.readUInt32BE(cursor);
    const compressedSize = table.readUInt32BE(cursor + 4);
    const flags = table.readUInt16BE(cursor + 8);
    cursor += 10;
    if (uncompressedSize > MAX_BLOCK_UNCOMPRESSED_BYTES)
      throw unityFsFormatFailure("declared block size exceeds its bound");
    if (
      (flags & UNITYFS_FLAG_COMPRESSION_MASK) !== UNITYFS_COMPRESSION_NONE &&
      compressedSize === 0
    )
      throw unityFsFormatFailure("compressed block declares no bytes");
    blocks.push({
      uncompressedSize,
      compressedSize,
      flags,
      compressedStart,
      streamStart,
    });
    compressedStart += compressedSize;
    streamStart += uncompressedSize;
  }
  return blocks;
};

const parseNodes = (
  table: Buffer,
  blocks: readonly UnityFsBlock[],
): UnityFsNode[] => {
  let cursor = 20 + blocks.length * 10;
  if (cursor + 4 > table.length)
    throw unityFsFormatFailure("node count is missing from the table");
  const nodeCount = table.readUInt32BE(cursor);
  cursor += 4;
  if (cursor + nodeCount * 21 > table.length)
    throw unityFsFormatFailure("declared node count exceeds the table");
  const streamSize =
    blocks.length === 0
      ? 0
      : (blocks[blocks.length - 1]?.streamStart ?? 0) +
        (blocks[blocks.length - 1]?.uncompressedSize ?? 0);
  const nodes: UnityFsNode[] = [];
  for (let index = 0; index < nodeCount; index++) {
    const offsetBig = table.readBigUInt64BE(cursor);
    const sizeBig = table.readBigUInt64BE(cursor + 8);
    const flags = table.readUInt32BE(cursor + 16);
    cursor += 20;
    const nameEnd = table.indexOf(0, cursor);
    if (nameEnd === -1) throw unityFsFormatFailure("node path is unterminated");
    let path: string;
    try {
      path = UTF8.decode(table.subarray(cursor, nameEnd));
    } catch (cause: unknown) {
      throw new ArtifactReaderFailure(
        "format",
        "UnityFS node path is not valid UTF-8",
        { cause },
      );
    }
    cursor = nameEnd + 1;
    if (path.length === 0 || path.includes("\0") || path.includes(".."))
      throw unityFsFormatFailure("empty or traversal member path");
    if (
      offsetBig > BigInt(streamSize) ||
      sizeBig > BigInt(streamSize) - offsetBig
    )
      throw unityFsFormatFailure(`member exceeds the stream: ${path}`);
    nodes.push({
      offset: Number(offsetBig),
      size: Number(sizeBig),
      flags,
      path,
    });
  }
  return nodes;
};

const resolveDataStart = (
  header: UnityFsHeader,
  blocks: readonly UnityFsBlock[],
  size: number,
): number => {
  let dataStart = header.blocksInfoAtEnd
    ? header.afterHeader
    : header.blocksInfoOffset + header.compressedBlocksInfoSize;
  if (
    !header.blocksInfoAtEnd &&
    (header.usesBlockAlignment ||
      (header.flags & UNITYFS_FLAG_BLOCK_INFO_NEEDS_PADDING) !== 0)
  )
    dataStart = align16(dataStart);
  const compressedTotal = blocks.reduce(
    (total, block) => total + block.compressedSize,
    0,
  );
  if (dataStart < 0 || dataStart + compressedTotal > size)
    throw unityFsFormatFailure("declared data area exceeds the file");
  return dataStart;
};

export const readUnityFsBlocksInfo = async (
  handle: FileHandle,
  header: UnityFsHeader,
  size: number,
  signal?: AbortSignal,
): Promise<UnityFsBlocksInfo> => {
  const compressed = await readPckBytes(
    handle,
    header.blocksInfoOffset,
    header.compressedBlocksInfoSize,
    size,
    signal,
  );
  const table = await decodeBlocksInfoTable(header, compressed, signal);
  if (table.length < 20)
    throw unityFsFormatFailure("blocks-info table is truncated");
  const blocks = parseBlocks(table);
  const nodes = parseNodes(table, blocks);
  return {
    dataHash: table.subarray(0, 16).toString("hex"),
    blocks,
    nodes,
    dataStart: resolveDataStart(header, blocks, size),
  };
};
