# Masstree -- a trie of cache aware lock-less B+ trees.

*WORK IN PROGRESS*

Masstree - a trie of cache aware lock-less B+ trees.

Based on design by Y. Mao, E. Kohler and R. Morris.  Reference:

        http://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf

- Keys are sliced into 64-bits per layer of the trie.
- Each layer is a B+ tree with fanout 16.
- Optimistic concurrency.
