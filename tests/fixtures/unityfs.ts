/** Synthetic UnityFS (AssetBundle) builders for reader tests. */

export interface UnityFsMember {
  readonly path: string;
  readonly data: string | Buffer;
  readonly flags?: number;
}

export interface UnityFsOptions {
  readonly formatVersion?: number;
  readonly playerVersion?: string;
  readonly engineVersion?: string;
  readonly flags?: number;
  readonly blocksInfoAtEnd?: boolean;
  readonly blocksInfoCompression?: number;
  readonly blockCompression?: number;
  readonly blockSize?: number;
  readonly declaredNodeCount?: number;
  readonly declaredBlockCount?: number;
}

const utf8 = (value: string): Buffer => Buffer.from(value, "utf8");
const cstring = (value: string): Buffer =>
  Buffer.concat([utf8(value), Buffer.alloc(1)]);

const align16 = (value: number): number => (value + 15) & ~15;

/** A valid LZ4 block made of literal-only sequences; no compression attempted. */
export const lz4LiteralBlock = (data: Buffer): Buffer => {
  const chunks: Buffer[] = [];
  let position = 0;
  while (position < data.length) {
    const run = Math.min(data.length - position, 0x10ffff);
    const token = run >= 15 ? 0xf0 : run << 4;
    const head: number[] = [token];
    if (run >= 15) {
      let extra = run - 15;
      do {
        head.push(Math.min(extra, 255));
        extra -= 255;
      } while (extra >= 0 && head[head.length - 1] === 255);
    }
    chunks.push(Buffer.from(head), data.subarray(position, position + run));
    position += run;
  }
  return Buffer.concat(chunks);
};

const maybeCompress = (data: Buffer, compression: number): Buffer =>
  compression === 2 || compression === 3 ? lz4LiteralBlock(data) : data;

interface BlockRecord {
  readonly uncompressedSize: number;
  readonly compressedSize: number;
  readonly flags: number;
}

/** Chunk the virtual stream into blocks the way a producer would. */
const chunkIntoBlocks = (
  stream: Buffer,
  blockSize: number,
  compression: number,
): { records: BlockRecord[]; payload: Buffer[] } => {
  const records: BlockRecord[] = [];
  const payload: Buffer[] = [];
  for (
    let offset = 0;
    offset < stream.length || (stream.length === 0 && offset === 0);
  ) {
    const uncompressed = stream.subarray(offset, offset + blockSize);
    if (uncompressed.length === 0 && stream.length !== 0) break;
    const compressed = maybeCompress(uncompressed, compression);
    records.push({
      uncompressedSize: uncompressed.length,
      compressedSize: compressed.length,
      flags: compression,
    });
    payload.push(compressed);
    offset += blockSize;
    if (stream.length === 0) break;
  }
  return { records, payload };
};

const buildTable = (
  members: readonly UnityFsMember[],
  contents: readonly Buffer[],
  blockRecords: readonly BlockRecord[],
  options: UnityFsOptions,
): Buffer => {
  let offsets = 0;
  const nodeRecords = members.map((member, index) => {
    const record = {
      offset: offsets,
      size: contents[index]?.length ?? 0,
      flags: member.flags ?? 0,
      path: member.path,
    };
    offsets += record.size;
    return record;
  });
  const parts: Buffer[] = [Buffer.alloc(16), Buffer.alloc(4)];
  parts[1]!.writeUInt32BE(options.declaredBlockCount ?? blockRecords.length, 0);
  for (const record of blockRecords) {
    const row = Buffer.alloc(10);
    row.writeUInt32BE(record.uncompressedSize, 0);
    row.writeUInt32BE(record.compressedSize, 4);
    row.writeUInt16BE(record.flags, 8);
    parts.push(row);
  }
  const countRow = Buffer.alloc(4);
  countRow.writeUInt32BE(options.declaredNodeCount ?? nodeRecords.length, 0);
  parts.push(countRow);
  for (const record of nodeRecords) {
    const row = Buffer.alloc(20);
    row.writeBigUInt64BE(BigInt(record.offset), 0);
    row.writeBigUInt64BE(BigInt(record.size), 8);
    row.writeUInt32BE(record.flags, 16);
    parts.push(row, cstring(record.path));
  }
  return Buffer.concat(parts);
};

export const buildUnityFs = (
  members: readonly UnityFsMember[],
  options: UnityFsOptions = {},
): Buffer => {
  const formatVersion = options.formatVersion ?? 8;
  const playerVersion = options.playerVersion ?? "5.x.x";
  const engineVersion = options.engineVersion ?? "2021.3.32f1c1";
  const blockCompression = options.blockCompression ?? 3;
  const blocksInfoCompression = options.blocksInfoCompression ?? 3;
  const blockSize = options.blockSize ?? 131072;
  const blocksInfoAtEnd = options.blocksInfoAtEnd ?? false;
  const flags =
    options.flags ??
    blocksInfoCompression |
      (blocksInfoAtEnd ? 0x80 : 0) |
      (formatVersion >= 7 ? 0x200 : 0) |
      0x40;

  const contents = members.map(({ data }) =>
    typeof data === "string" ? utf8(data) : data,
  );
  const { records: blockRecords, payload: compressedBlocks } = chunkIntoBlocks(
    Buffer.concat(contents),
    blockSize,
    blockCompression,
  );
  const table = buildTable(members, contents, blockRecords, options);
  const compressedTable = maybeCompress(table, blocksInfoCompression);

  const headerPrefix = Buffer.concat([
    Buffer.from("UnityFS\0", "latin1"),
    Buffer.alloc(4),
  ]);
  headerPrefix.writeUInt32BE(formatVersion, 8);
  const headerStrings = Buffer.concat([
    cstring(playerVersion),
    cstring(engineVersion),
  ]);
  let position = headerPrefix.length + headerStrings.length + 20;
  if (formatVersion >= 7) position = align16(position);

  const dataArea = Buffer.concat(compressedBlocks);
  let dataStart: number;
  let blocksInfoOffset: number;
  if (blocksInfoAtEnd) {
    dataStart = position;
    blocksInfoOffset = position + dataArea.length;
  } else {
    blocksInfoOffset = position;
    dataStart =
      formatVersion >= 7 || (flags & 0x200) !== 0
        ? align16(position + compressedTable.length)
        : position + compressedTable.length;
  }
  const totalSize = Math.max(
    dataStart + dataArea.length,
    blocksInfoAtEnd ? blocksInfoOffset + compressedTable.length : 0,
    dataStart,
  );

  const file = Buffer.alloc(totalSize);
  headerPrefix.copy(file, 0);
  headerStrings.copy(file, headerPrefix.length);
  const fixed = headerPrefix.length + headerStrings.length;
  file.writeBigUInt64BE(BigInt(totalSize), fixed);
  file.writeUInt32BE(compressedTable.length, fixed + 8);
  file.writeUInt32BE(table.length, fixed + 12);
  file.writeUInt32BE(flags, fixed + 16);
  compressedTable.copy(file, blocksInfoOffset);
  dataArea.copy(file, dataStart);
  return file;
};
