# Masstree: lockless cache-aware tree [experimental]

Masstree is a lockless cache-aware trie of B+ trees.  This project provides
an implementation written in C11, distributed under the 2-clause BSD license.
Based on the design by Y. Mao, E. Kohler and R. Morris.  Reference:

        http://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf

**WARNING**:
- This code is experimental and is known to be unstable.  Use it at your own
risk.  Support is not provided at this stage.
- The concurrent delete operation is known to have bugs.
- Certain optimisations and features from the original paper are not
implemented.

**NOTE**: For a stable alternative, please see the
[concurrent trie-hash map](https://github.com/rmind/thmap) project.
