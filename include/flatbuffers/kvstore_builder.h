#pragma once

#include "flatbuffers/flatbuffers.h"

namespace flatbuffers {

// Like FlatBufferBuilder, but optimized
// for a key-value store.
// Usage:
//
// KVStoreBuilder kvb;
// ConcreteVBuilder ckvb(kvb);
//
// ckvb.add_k1(key1);
// ckvb.add_k2(key2);
// ckvb.add_k3(key3);
//
// ckvb.add_v1(val1);
// ckvb.add_v2(val2);
// ckvb.add_v3(val3);
//
// It maintains two buffers internally - one
// for key, one for value. In the example
// above, k1-k3 go to the first buffer and v1-v3
// go to the second buffer.
//
// However, key strings and key vectors are an exception
// The actual data goes into the key and the
// metadata (length and relative offset) goes into value.
//
// The order in which add_*() is called needs to match
// the order which they are defined in the schema.
class KVStoreBuilder {
 public:
  explicit KVStoreBuilder(uoffset_t initial_size = 1024,
                          const simple_allocator *allocator = nullptr)
  : key_builder_(initial_size, allocator)
  , value_builder_(initial_size, allocator) {}


  void Clear() {
    key_builder_.Clear();
    value_builder_.Clear();
  }

  uoffset_t GetSize() const {
    uoffset_t padding = 16; // TODO: Compute this at runtime
    return GetKeySize() + GetValueSize() + padding;
  }

  uoffset_t GetKeySize() const { return key_builder_.GetSize(); }
  uoffset_t GetValueSize() const { return value_builder_.GetSize(); }

  template<typename T> void AddElement(voffset_t field, T e, T def) {
    // Check if the field is key or value
    // then call AddElement on one of the two builders
    // Handle string/vectors special case. See comment at the beginning
    // of the class
    key_builder_.AddElement<T>(field, e, def);
  }

  unique_ptr_t ReleaseBufferPointer() {
    key_builder_.Finished();
    value_builder_.Finished();
    // TODO: we need to make sure that the two builders
    // have their memory "almost" contiguously, so
    // we can return a single GetSize()
    value_builder_.ReleaseBufferPointer();
    return key_builder_.ReleaseBufferPointer();
  }

  uoffset_t StartTable() {
    return 0; // Not implemented
  }

  uoffset_t EndTable(uoffset_t start, voffset_t numfields) {
    // TODO: generalize to value_builder_
    return key_builder_.EndTable(start, numfields);
  }

 private:
  FlatBufferBuilder key_builder_;
  FlatBufferBuilder value_builder_;
};

}  // namespace flatbuffers
