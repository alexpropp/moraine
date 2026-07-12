// RAII wrapper for a caller-owned array from a moraine_*_free-paired ABI
// call. Shared by catalog.cpp (listing ABI) and metadata_tables.cpp (dump
// ABI) — same ownership shape, one definition.
#pragma once

#include <cstddef>

namespace moraine_duckdb {

template <typename T> class OwnedArray {
public:
	explicit OwnedArray(void (*free_fn)(T *, size_t)) : items_(nullptr), len_(0), free_fn_(free_fn) {
	}
	~OwnedArray() {
		free_fn_(items_, len_);
	}
	OwnedArray(const OwnedArray &) = delete;
	OwnedArray &operator=(const OwnedArray &) = delete;

	T **OutItems() {
		return &items_;
	}
	size_t *OutLen() {
		return &len_;
	}
	T *begin() const {
		return items_;
	}
	T *end() const {
		return items_ + len_;
	}
	size_t size() const {
		return len_;
	}

private:
	T *items_;
	size_t len_;
	void (*free_fn_)(T *, size_t);
};

} // namespace moraine_duckdb
