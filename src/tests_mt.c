/*
 * Copyright (c) 2015 Mindaugas Rasiukevicius <rmind at netbsd org>
 * All rights reserved.
 *
 * Use is subject to license terms, as specified in the LICENSE file.
 */

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <err.h>

#include "masstree.c"

static masstree_t *		tree;
static pthread_barrier_t	barrier;
static unsigned			nworkers;

static void *
fuzz_put_get(void *arg)
{
	const unsigned id = (uintptr_t)arg;
	unsigned n = 1000 * 1000;

	pthread_barrier_wait(&barrier);
	while (n--) {
		uint64_t key = random() & 0xfff;

		if (random() & 0x1) {
			masstree_put(tree, &key, sizeof(key),
			    (void *)(uintptr_t)key);
		} else {
			void *val = masstree_get(tree, &key, sizeof(key));
			assert(!val || (uintptr_t)val == (uintptr_t)key);
		}
	}
	return NULL;
}

static void *
fuzz_put_del(void *arg)
{
	const unsigned id = (uintptr_t)arg;
	unsigned n = 1000 * 1000;

	pthread_barrier_wait(&barrier);
	while (n--) {
		uint64_t key = random() & 0x1f;

		if (random() & 0x1) {
			masstree_put(tree, &key, sizeof(key), (void *)1);
		} else {
			masstree_del(tree, &key, sizeof(key));
		}
	}
	pthread_exit(NULL);
	return NULL;
}

static void
run_test(void *func(void *))
{
	pthread_t *thr;

	srandom(1);
	tree = masstree_create(NULL);
	nworkers = sysconf(_SC_NPROCESSORS_CONF) + 1;

	thr = malloc(sizeof(pthread_t) * nworkers);
	pthread_barrier_init(&barrier, NULL, nworkers);

	for (unsigned i = 0; i < nworkers; i++) {
		if ((errno = pthread_create(&thr[i], NULL,
		    func, (void *)(uintptr_t)i)) != 0) {
			err(EXIT_FAILURE, "pthread_create");
		}
	}
	for (unsigned i = 0; i < nworkers; i++) {
		pthread_join(thr[i], NULL);
	}
	pthread_barrier_destroy(&barrier);
}

int
main(void)
{
	run_test(fuzz_put_get);
	run_test(fuzz_put_del);
	puts("ok");
	return 0;
}
