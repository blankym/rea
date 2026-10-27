/**
 * UnityFS (AssetBundle) container structure: a fixed signature header followed
 * by a compressed blocks-and-directory table and a block-compressed data area.
 * All multi-byte fields in the header and table are big-endian.
 */
export const UNITYFS_SIGNATURE = Buffer.from("UnityFS\0", "latin1");

/** Whether bytes start with a standalone UnityFS header. */
export const hasUnityFsHeader = (bytes: Uint8Array): boolean =>
  bytes.length >= UNITYFS_SIGNATURE.length &&
  Buffer.from(bytes.buffer, bytes.byteOffset, UNITYFS_SIGNATURE.length).equals(
    UNITYFS_SIGNATURE,
  );

/**
 * Header dataflags values for format versions produced by Unity 2019.3+.
 * Older layouts reuse 0x200 for Unity-CN block encryption; readers report
 * encrypted data through the same unavailable status either way.
 */
export const UNITYFS_FLAG_COMPRESSION_MASK = 0x3f;
export const UNITYFS_FLAG_BLOCKS_INFO_AT_END = 0x80;
export const UNITYFS_FLAG_BLOCK_INFO_NEEDS_PADDING = 0x200;
export const UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION = 0x1400;
export const UNITYFS_FLAG_USES_BUNDLE_ENCRYPTION_LEGACY = 0x200;

/** Per-block compression values in the low six flag bits. */
export const UNITYFS_COMPRESSION_NONE = 0;
export const UNITYFS_COMPRESSION_LZMA = 1;
export const UNITYFS_COMPRESSION_LZ4 = 2;
export const UNITYFS_COMPRESSION_LZ4HC = 3;

/** Supported format revisions; newer headers keep this layout but may add flags. */
export const UNITYFS_FORMAT_VERSION_MIN = 6;
export const UNITYFS_FORMAT_VERSION_MAX = 9;

/** Header is aligned to 16 bytes for format 7+ and for 2019.4.15+ producers. */
export const unityfsHeaderAligned = (
  formatVersion: number,
  engineMajor: number | undefined,
  engineMinor: number | undefined,
  enginePatch: number | undefined,
): boolean =>
  formatVersion >= 7 ||
  (engineMajor === 2019 &&
    engineMinor === 4 &&
    enginePatch !== undefined &&
    enginePatch >= 15);

/** Producer version strings are short; the bound keeps a corrupt header local. */
export const UNITYFS_HEADER_STRING_MAX = 1024;

/** Declared header fields; support and extent admission belong to callers. */
export interface UnityFsHeaderFacts {
  readonly formatVersion: number;
  readonly playerVersion: string;
  readonly engineVersion: string;
  readonly declaredSize: bigint;
  readonly compressedBlocksInfoSize: number;
  readonly uncompressedBlocksInfoSize: number;
  readonly flags: number;
  /** Byte offset just past the fixed tail, before any block alignment. */
  readonly afterFixed: number;
}

const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/**
 * Decode declared fields after the caller admits a prefix covering the
 * variable-length header. Undefined means the admitted bytes cannot carry a
 * complete header; signature absence stays the caller's earlier check.
 */
export const readUnityFsHeaderFacts = (
  bytes: Uint8Array,
): UnityFsHeaderFacts | undefined => {
  if (!hasUnityFsHeader(bytes)) return undefined;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let cursor = 12;
  const readString = (): string | undefined => {
    const start = cursor;
    while (
      cursor < bytes.length &&
      cursor - start < UNITYFS_HEADER_STRING_MAX
    ) {
      if (bytes[cursor] === 0) {
        const value = bytes.subarray(start, cursor);
        cursor += 1;
        try {
          return utf8.decode(value);
        } catch {
          return undefined;
        }
      }
      cursor += 1;
    }
    return undefined;
  };
  const playerVersion = readString();
  const engineVersion = readString();
  if (
    playerVersion === undefined ||
    engineVersion === undefined ||
    cursor + 20 > bytes.length
  )
    return undefined;
  const fixed = cursor;
  return {
    formatVersion: view.getUint32(8),
    playerVersion,
    engineVersion,
    declaredSize: view.getBigUint64(fixed),
    compressedBlocksInfoSize: view.getUint32(fixed + 8),
    uncompressedBlocksInfoSize: view.getUint32(fixed + 12),
    flags: view.getUint32(fixed + 16),
    afterFixed: fixed + 20,
  };
};
