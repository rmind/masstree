/*
 * Copyright (c) 2014-2015 Mindaugas Rasiukevicius <rmind at netbsd org>
 * All rights reserved.
 *
 * Use is subject to license terms, as specified in the LICENSE file.
 */

#include "masstree.c"

/*
 * Some utility routines to ease the debugging.
 */

static uint64_t
string_to_key(const char *s, size_t *len)
{
	uint64_t key = 0;

	*len = strlen(s), memcpy(&key, s, *len);
	return htobe64(key);
}

static void
free2(void *ptr, size_t len)
{
	(void)len;
	free(ptr);
}

static const masstree_ops_t	ops = { .alloc = malloc, .free = free2 };

/*
 * Unit tests.
 */

static void
node_locking_test(void)
{
	mtree_node_t node_store, *node = &node_store;
	uint32_t v, c;

	memset(node, 0, sizeof(mtree_node_t));
	v = stable_version(node);
	assert(v == 0);

	/* Test the lock. */
	lock_node(node);
	v = stable_version(node);
	assert(v == NODE_LOCKED);

	/* Test NODE_VINSERT counter. */
	node->version |= NODE_INSERTING;
	unlock_node(node);
	c = (node->version & NODE_VINSERT) >> NODE_VINSERT_SHIFT;
	assert(c == 1);

	/* Test NODE_VSPLIT counter. */
	lock_node(node);
	node->version |= NODE_SPLITTING;
	unlock_node(node);
	c = (node->version & NODE_VSPLIT) >> NODE_VSPLIT_SHIFT;
	assert(c == 1);

	/* The flags should be cleared and the lock is not held. */
	c = (1 << NODE_VINSERT_SHIFT) | (1 << NODE_VSPLIT_SHIFT);
	v = stable_version(node);
	assert(v == c);
}

static void
leaf_ops_test(void)
{
	masstree_t *tree = masstree_create(&ops);
	mtree_leaf_t *leaf = leaf_create(tree);
	mtree_node_t *node = (mtree_node_t *)leaf;
	size_t len;

	lock_node(node);
	leaf->version |= NODE_INSERTING;

	for (unsigned i = 0; i < 3; i++) {
		const char skey[2] = { 'X' - i, 0 };
		uint64_t key = string_to_key(skey, &len);
		void *val = (void *)((uintptr_t)i + 2);

		leaf_insert_key(node, key, len, val);
	}
	unlock_node(node);

	uint64_t key = string_to_key("X", &len);
	unsigned type, idx = leaf_find_lv(leaf, key, len, &type);
	void *val = leaf->lv[idx];

	assert(type == MTREE_VALUE);
	assert((uintptr_t)val == 2);
}

static void
inode_ops_test(void)
{
	masstree_t *tree = masstree_create(&ops);
	mtree_inode_t *inode = internode_create(tree);
	mtree_node_t *node = (mtree_node_t *)inode;
	uint64_t keys[] = { 1, 2, 3 };

	lock_node(node);
	inode->version |= NODE_INSERTING;
	for (unsigned i = 0; i < (sizeof(keys) / sizeof(keys[0])); i++) {
		mtree_node_t *child = (mtree_node_t *)leaf_create(tree);

		lock_node(child);
		internode_insert(node, keys[i], child);
		unlock_node(child);
	}
	internode_remove(node, 1);
	unlock_node(node);
}

/*
 * Random insert/lookup/check test.
 */

static void
random_keyval_test(void)
{
	for (unsigned s = 1; s <= 100; s++) {
		masstree_t *tree = masstree_create(&ops);
		size_t n = 10000, i = n;

		srandom(s);
		while (--i) {
			unsigned long val = random(), key = random() % 1000;
			void *pval = (void *)(uintptr_t)val, *pkey = &key;

			masstree_put(tree, pkey, sizeof(key), pval);
			pval = masstree_get(tree, pkey, sizeof(key));

			if ((unsigned long)(uintptr_t)pval != val) {
				printf("%llx, %llx, %u\n", val, key, n - i);
				abort();
			}
		}
	}
}

int
main(void)
{
	node_locking_test();
	leaf_ops_test();
	inode_ops_test();

	random_keyval_test();

	puts("ok");
	return 0;
}
