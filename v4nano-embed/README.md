# v4nano-embed

A deliberately narrow ONNX runner for **one** embedding model:
[`voyageai/voyage-4-nano`](https://huggingface.co/voyageai/voyage-4-nano)
via the [`onnx-community/voyage-4-nano-ONNX`](https://huggingface.co/onnx-community/voyage-4-nano-ONNX)
int8 export.

It exists because moofile's autoembedding used to run embeddings through
`fastembed`, whose registry only knows the models compiled into it. Rather than
fork fastembed to add one model, we extracted the ~100 lines of glue fastembed
uses to run an ONNX text-embedding model and hardcoded the rest.

There is no registry, no downloader, no batching strategy selection — just
"load this model, mean-pool its `last_hidden_state`, L2-normalize".

```rust
use v4nano_embed::V4Nano;
let mut m = V4Nano::load("model_quantized.onnx", "tokenizer.json", 32768, None)?;
let v = m.embed(&["hello world".to_string()])?;
assert_eq!(v[0].len(), v4nano_embed::DIM); // 2048
```

- Output: 2048-dim, L2-normalized. Truncate to 1024/512/256 yourself for MRL.
- Query prefix is the caller's job (voyage-4-nano wants
  `Represent the query for retrieving supporting documents: ` on the query side;
  documents are unprefixed).
- Tokenizer glue + mean-pooling/normalization adapted from
  [`fastembed`](https://github.com/Anush008/fastembed-rs) (Apache-2.0); see NOTICE.

## License

Apache-2.0. moofile (the parent repo) is MIT; this crate is the one that
carries Apache-2.0 code, which is why it's a separate crate rather than a
module inside `core/`.
