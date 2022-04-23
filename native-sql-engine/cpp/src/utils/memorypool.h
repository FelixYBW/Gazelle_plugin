/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <sys/mman.h>

#include <arrow/memory_pool.h>
#include <jemalloc/jemalloc.h>

class ARROW_EXPORT LargePageMemoryPool : public MemoryPool {
 public:
  explicit LargePageMemoryPool(MemoryPool* pool):pool_(pool){}

  ~LargePageMemoryPool() override = default;

  Status Allocate(int64_t size, uint8_t** out) override
  {
    if (size <2*1024*1024)
    {
      return pool_->Allocate(size, out);
    }
#ifdef ARROW_JEMALLOC

    *out = reinterpret_cast<uint8_t*>(
        mallocx(static_cast<size_t>(size), MALLOCX_ALIGN(2*1024*1024)));
    if (*out == NULL) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
    madvise(*out, size, /*MADV_HUGEPAGE */ 14);
    return Status::OK();
#else
    return pool_->Allocate(size, out);
#endif
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override{
    if (new_size <2*1024*1024)
    {
      return pool_->Reallocate(old_size, new_size, ptr);
    }
#ifdef ARROW_JEMALLOC

    *ptr = reinterpret_cast<uint8_t*>(
        rallocx(*ptr, static_cast<size_t>(new_size), MALLOCX_ALIGN(2*1024*1024)));
    if (*ptr == NULL) {
      *ptr = previous_ptr;
      return Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    madvise(*ptr, size, /*MADV_HUGEPAGE */ 14);
    return Status::OK();
#else
    return pool_->Reallocate(old_size, new_size, ptr);
#endif
  }

  void Free(uint8_t* buffer, int64_t size) override{
    return pool_->Free(buffer, size);
  }

  int64_t bytes_allocated() const override{
    return pool_->bytes_allocated();
  }

  int64_t max_memory() const override{
    return pool_->max_memory();
  }

  std::string backend_name() const override{
    return "LargePageMemoryPool";
  }

  void set_pool(MemoryPool* pool)
  {
    pool_=pool;
  }

 private:
  MemoryPool* pool_;
};

