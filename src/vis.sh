#!/bin/sh

cin="$1"
set -eu

in="/tmp/__masstree.dot"
if [ -z "$cin" ]; then
	./masstree_vis > $in <<-EOF
	SET cat 1
	SET cats 2
	SET cats_are_cute 3
	SET cats_are_fast 4
	SET cats_are_sleepy 5
	SET cats_in_the_garden 6
	SET cats_on_the_street 7
	SET catnip 8
	SET cattle 9
	SET cat_on_a_hot_tin_roof 10
	EOF
else
	in="$cin"
fi

dot -Tpng $in -o masstree.png
