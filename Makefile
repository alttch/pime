VERSION=0.0.17

all:
	echo "Select target"

tag:
	git tag -a v${VERSION} -m v${VERSION}
	git push origin --tags

ver:
	sed -i 's/^version = ".*/version = "${VERSION}"/g' Cargo.toml

doc:
	grep -v "^//!" src/lib.rs > src/lib.rs.tmp
	sed 's|^|//! |g' README.md > src/lib.rs
	cat src/lib.rs.tmp >> src/lib.rs
	rm -f src/lib.rs.tmp
	cargo doc

pub: doc publish-cargo-crate tag

publish-cargo-crate:
	cargo publish
