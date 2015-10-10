/*
 * Copyright (c) 2014-2015 Mindaugas Rasiukevicius <rmind at netbsd org>
 * All rights reserved.
 *
 * Use is subject to license terms, as specified in the LICENSE file.
 */

#include <stdio.h>
#include <string.h>

#include "masstree.c"

static unsigned
split(char *line, const char *sep, char **p, unsigned max)
{
	unsigned n = 0;
	char *token;

	while ((token = strsep(&line, sep)) != NULL) {
		p[n++] = token;
		if (n == max)
			break;
	}
	return n;
}

#define	STRING	1

static void
dump_node(mtree_node_t *node)
{
	if (node->version & NODE_ISBORDER) {
		mtree_leaf_t *leaf = cast_to_leaf(node);
		unsigned nkeys = PERM_NKEYS(leaf->permutation);

		printf("  node%p[label=\"", node);
		for (unsigned i = 0; i < nkeys; i++) {
			unsigned slot = PERM_KEYIDX(leaf->permutation, i);
			uint64_t key = htobe64(leaf->keyslice[slot]);
			char sep = (i + 1) == nkeys ? ' ' : '|';
#if STRING
			char skey[9] = { [8] = 0 };
			memcpy(skey, &key, sizeof(key));
			printf("<f%u> %s%c", i, (const char *)skey, sep);
#else
			printf("<f%u> %lld%c", i, htobe64(key), sep);
#endif
		}
		printf("\"];\n");

		for (unsigned i = 0; i < nkeys; i++) {
			unsigned slot = PERM_KEYIDX(leaf->permutation, i);
			void *lv = leaf->lv[slot];

			if (leaf->keyinfo[slot] & MTREE_LAYER) {
				printf("  node%p:f%u -> node%p [style=dotted]\n",
				    node, i, lv);
				continue;
			}
			printf("  node%p:f%u -> val%p\n",
			    node, i, &leaf->lv[slot]);
			printf("  val%p [label=%u,shape=plaintext]\n",
			    &leaf->lv[slot], (unsigned)(uintptr_t)lv);
		}

		for (unsigned i = 0; i < nkeys; i++) {
			unsigned slot = PERM_KEYIDX(leaf->permutation, i);

			if (leaf->keyinfo[slot] & MTREE_LAYER) {
				mtree_node_t *layer = leaf->lv[slot];
				dump_node(layer);
			}
		}

	} else {
		mtree_inode_t *inode = cast_to_inode(node);
		unsigned nkeys = inode->nkeys;

		printf("  node%p[label = \"", node);
		for (unsigned i = 0; i < nkeys; i++) {
			uint64_t key = htobe64(inode->keyslice[i]);
#if STRING
			printf("<f%u> |%s|", i, (const char *)&key);
#else
			printf("<f%u> |%lld|", i, htobe64(key));
#endif
		}
		printf("<f%u>\"];\n", nkeys);

		for (unsigned i = 0; i <= nkeys; i++) {
			printf("  node%p:f%u -> node%p\n",
			    node, i, inode->child[i]);
			dump_node(inode->child[i]);
		}
	}
}


static void
dump_tree(masstree_t *tree)
{
	/* Dump the tree in DOT format for Graphviz. */
	puts("digraph btree {");
	puts("  node [shape = record];");
	dump_node(tree->root);
	puts("}");
}

int
main(int argc, char **argv)
{
	masstree_t *tree = masstree_create(NULL);
	char *line = NULL;
	size_t linecap = 0;
	ssize_t len;

	while ((len = getline(&line, &linecap, stdin)) > 0) {
		char *p[3], *key, *valp;
		unsigned c;

		if ((c = split(line, " ", p, 3)) < 2) {
			continue;
		}
		line[len - 1] = '\0';
		key = p[1];

		if (strcmp(p[0], "SET") == 0) {
			unsigned long val = atol(p[2]);
			valp = (void *)(uintptr_t)val;
			masstree_put(tree, key, strlen(key), valp);
		}
		if (strcmp(p[0], "GET") == 0 && c > 1) {
			valp = masstree_get(tree, key, strlen(key));
			printf("%lu\n", (unsigned)(uintptr_t)valp);
		}
	}

	dump_tree(tree);

	return 0;
}
