# Masstree: lockless cache-aware tree

*WORK IN PROGRESS*

Masstree is a lockless cache-aware trie of B+ trees.  This project provides
an implementation written in C11, distributed under the 2-clause BSD license.
Based on the design by Y. Mao, E. Kohler and R. Morris.  Reference:

        http://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf

Highlights:
- Very competitive performance, especially with the long keys.
- High concurrency using optimistic locking and lock-free techniques.
- Cache-aware layout of the tree.
