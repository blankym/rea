# UnityFS AssetBundle archives

`inspect-artifact` and `extract-artifact` read standalone UnityFS AssetBundle
containers without running Unity or the producer application. Header format
versions 6 through 9 (Unity 2019.3 and later producers) are parsed; bundles
declaring other versions fail as unsupported rather than being misread.

Inspection retains the header facts: format version, player and engine version
strings, declared size, dataflags, blocks-and-directory table bounds, the
producer's 16-byte data hash as declared, and the resolved data-area offset.
Members (`CAB-*` SerializedFiles, `.resS` resource streams, shared assets)
are listed with their declared source paths, stream offsets, sizes and node
flags. Member contents are streamed on demand: only the blocks covering the
requested member are read and decompressed.

Data blocks compressed with none, LZ4 or LZ4HC are supported. LZMA-compressed
blocks and blocks-info tables are reported as unsupported coverage or an
`unavailable` member read; the directory and any readable members are still
retained. Bundles declaring asset-bundle encryption keep their header facts
but report unknown directory coverage; no decryption is attempted. A declared
size that exceeds the input file, node paths outside the block stream, and
counts that cannot fit their table are malformed-input errors.

## CLI

```sh
rea inspect-artifact assets/hotfix.bundle --detail summary \
  --evidence_output evidence/hotfix.json --json
rea extract-artifact assets/hotfix.bundle --json
```

Detection is byte-based on the `UnityFS\0` signature, so `.bundle`, `.ab`,
`assets_*` YooAsset bundles and extensionless files all classify as `unityfs`
containers. Compact delivery keeps complete Evidence in the requested bundle.

## Verification scope

Synthetic fixtures cover LZ4HC and uncompressed tables, blocks-info at the
end, format 6 layouts without padding alignment, members spanning multiple
blocks, malformed counts, unsupported LZMA members, cancellation and
abandoned streams. One real YooAsset bundle produced by Unity 2021.3.32f1
(format 8, LZ4HC) was additionally verified: the streamed SerializedFile
contained its embedded managed assembly byte-identical to a UnityPy
extraction. Other producer variants have not been verified.
