// This is a generic module used by the transaction generator to produce
// multiple module to publish and use.
// The idea is that by running `cargo run --package module-publish`
// in `testsuite/module-publish`
// a rust file (`generic_module.rs`) gets generated in
// `crates/transaction-emitter-lib/src/transaction_generator/publishing` which
// contains a `CompiledModule` for the module below.
// A helper file is provided to manipulate that file to generate
// multiple, publishable module.
module 0xABCD::Simple {
    use aptos_framework::account;
    use std::signer;
    use aptos_framework::coin;
    use aptos_framework::managed_coin;
    use std::error;
    use std::vector;
    use std::table::{Self, Table};

    // Through the constant pool it will be possible to change this
    // constant to be as big or as small as desired.
    // That would affect the size of the module being published
    // and the cost of loading a constant.
    const RANDOM: vector<u64> = vector<u64>[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

    // Load and return a value from the constant `RANDOM`.
    // No data read or write.
    // Constant load test.
    public entry fun get_from_random_const(_s: &signer, idx: u64) {
        let length = vector::length(&RANDOM);
        if (length != 0) {
            if (idx >= length) {
                idx = length - 1;
            };
            *vector::borrow(&RANDOM, idx);
        }
    }

    //
    // Simple entries no data touched except for signer account.
    //

    // No operation, in principle tests everything except computation,
    // though there is a lot happening to get to this point.
    // In a sense compute the cost of an "empty" transaction.
    public entry fun nop(_s: &signer) {
    }

    // Test simple CPU usage. Loop as defined by the input `count`.
    // Not a true test of CPU usage given the number of instructions
    // used, but a simple reference to computation with no data access.
    public entry fun loopy(_s: &signer, count: u64) {
        while (count > 0) {
            count = count - 1;
        }
    }

    // Counter
    // This is a constant to change to check versioning of the module published.
    // In a simple way this can be used as a verion info, and incremented by 1
    // every time an upgrade is performed.
    // There are 2 usages to verify that change:
    // - simply call `get_counter` to see that is the value expected
    // - call `step` as many times as desiered on different version and
    //   then check the the stores Counter value is the expected one
    //  (more cumbersome, but with some information about correctness
    //  over time: if `Counter` is the proper sum of all `steps` called
    //  at the different versions).
    const COUNTER_STEP: u64 = 1;

    struct Counter has key {
        count: u64,
    }

    // Create the global `Counter`.
    // Stored under the module publisher address.
    fun init_module(publisher: &signer) {
        move_to<Counter>(
            publisher,
            Counter { count: 0 }
        );
    }

    // Update `Counter` (private to the module, effectively a `static` in more
    // classic langages) by `COUNTER_STEP`.
    // The idea is that `COUNTER_STEP` is one of the few values (if not the only
    // one) that changes across versions.
    public entry fun step(s: &signer) acquires Counter {
        let counter = borrow_global_mut<Counter>(signer::address_of(s));
        *(&mut counter.count) = counter.count + COUNTER_STEP;
    }

    // Get the value behind `Counter`.
    public entry fun get_counter(s: &signer) acquires Counter {
        let counter = borrow_global<Counter>(signer::address_of(s));
        counter.count;
    }

    //
    // Resource
    //

    struct ByteResource has key {
        data: vector<u8>,
    }

    public entry fun bytes_make_or_change(owner: &signer, data: vector<u8>) acquires ByteResource {
        if (exists<ByteResource>(signer::address_of(owner))) {
            let resource = borrow_global_mut<ByteResource>(signer::address_of(owner));
            *(&mut resource.data) = data;
        } else {
            let resource = ByteResource { data };
            move_to<ByteResource>(owner, resource);
        }
    }

    // used to initialize `Resource`
    const NAME: vector<u8> = b"hello";
    const DATA: vector<u8> = x"0123456789ABCDEF";

    struct Resource has key {
        id: u64,
        name: String,
        data: Data,
    }

    struct Data has copy, drop, store {
        data: vector<u8>,
    }

    // Update `Resource` with the values provided. This is effectively a rewrite of `Resource`.
    // If `Resource` does not exist at the signer's address, it is generated.
    // Read is size of `Resource`.  Write is a function of the transaction size.
    // The transaction size is the size of the data rewritten.
    public entry fun make_or_change(owner: &signer, id: u64, name: String, data: vector<u8>) acquires Resource {
        if (exists<Resource>(signer::address_of(owner))) {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            *(&mut resource.id) = id;
            *(&mut resource.name) = name;
            *(&mut resource.data.data) = data;
        } else {
            let data = Data { data };
            let resource = Resource { id, name, data };
            move_to<Resource>(owner, resource);
        }
    }

    // Reset Resource to default values (see code).
    // Create `Resource` if one does not exist at signer's address.
    // Read is size of `Resource`. Write is likely a smaller value.
    // Transaction size is small and constant.
    public entry fun reset_data(owner: &signer) acquires Resource {
        if (exists<Resource>(signer::address_of(owner))) {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            *(&mut resource.id) = 0;
            *(&mut resource.name) = utf8(NAME);
            *(&mut resource.data.data) = DATA;
        } else {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name: string::utf8(NAME),
                data
            };
            move_to<Resource>(owner, resource);
        }
    }

    // Given an `other` address with `Resource` (if `Resource` does not exist on `other`,
    // this funtion returns immediately) it appends the bigger `Resource.data`- between
    // `other` and `owner` (`signer`) - to the the smaller.
    // On return the owner of the smaller `Resource` will have its value bigger than
    // the other (the bigger value get appended to the smaller one) or at least 1K.
    // Read is size of 2 rousources/addresses. Write is 1 resource/address (bigger than what was there).
    // Transaction size is small and constant.
    public entry fun maximize(owner: &signer, other: address) acquires Resource {
        if (exists<Resource>(other)) {
            // not much to do, in principle the testing framework is calling on address with Resource
            return
        };
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name: string::utf8(NAME),
                data
            };
            move_to<Resource>(owner, resource);
        };
        // get data length for owner (len1) and address (len2)
        let len1 = {
            let resource = borrow_global<Resource>(signer::address_of(owner));
            vector::length(&resource.data.data)
        };
        let len2 = {
            let resource = borrow_global<Resource>(other);
            vector::length(&resource.data.data)
        };
        // get the data that is bigger and the destination that is the
        // smaller of the 2.
        let (data, resource) = if (len1 > len2) {
            let data = {
                let resource = borrow_global<Resource>(signer::address_of(owner));
                resource.data.data
            };
            (data, borrow_global_mut<Resource>(other))
        } else {
            let data = {
                let resource = borrow_global<Resource>(other);
                resource.data.data
            };
            (data, borrow_global_mut<Resource>(signer::address_of(owner)))
        };
        // copy the biger data into the smaller one
        while (
            vector::length(&data) > vector::length(&resource.data.data) ||
            vector::length(&resource.data.data) < 10000
        ) {
            append_data(&mut resource.data.data, &data);
        }
    }

    fun append_data(dest: &mut vector<u8>, source: &vector<u8>) {
        let len = vector::length(source);
        while (len > 0) {
            vector::push_back(dest, *vector::borrow(source, len - 1));
            len = len - 1;
        }
    }

    // Given an `other` address with `Resource` (if `Resource` does not exist on `other`,
    // this funtion returns immediately) it resizes the bigger `Resource.data` - between
    // `other` and `owner` (`signer`) - to half the size of the smaller.
    // On return the owner of the bigger `Resource` will have its size half of the smaller size.
    // Read is size of 2 rousources/addresses. Write is 1 resource/address (smaller than what was there).
    // Transaction size is small and constant.
    public entry fun minimize(owner: &signer, other: address) acquires Resource {
        if (exists<Resource>(other)) {
            // not much to do, in principle the testing framework is calling on address with Resource
            return
        };
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name: string::utf8(NAME),
                data
            };
            move_to<Resource>(owner, resource);
        };
        let (len1, len2) = {
            let resource1 = borrow_global<Resource>(signer::address_of(owner));
            let resource2 = borrow_global<Resource>(other);
            (vector::length(&resource1.data.data), vector::length(&resource2.data.data))
        };
        let (new_len, resource) = if (len1 > len2) {
            (len2 / 2, borrow_global_mut<Resource>(signer::address_of(owner)))
        } else {
            (len1 / 2, borrow_global_mut<Resource>(other))
        };
        while (vector::length(&resource.data.data) > new_len) {
            vector::pop_back(&mut resource.data.data);
        }
    }

    // Set the `Resource.id` to the value provided.
    // Create `Resource` if one does not exist at signer's address.
    // Read is size of `Resource`. Write is size of `Resource` too.
    // Transaction size is small and constant.
    public entry fun set_id(owner: &signer, id: u64) acquires Resource {
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id,
                name: string::utf8(NAME),
                data,
            };
            move_to<Resource>(owner, resource);
        } else {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            resource.id = id;
        }
    }

    // Set `Resource.name` to the value provided.
    // Create `Resource` if one does not exist at signer's address.
    // Read is size of `Resource`. Write is the size with the field `name` updated.
    // Transaction size is function of `name` in input.
    public entry fun set_name(owner: &signer, name: String) acquires Resource {
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name,
                data,
            };
            move_to<Resource>(owner, resource);
        } else {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            resource.name = name;
        }
    }

    // Double `Resource.data`. When `.data` is big enough, it effectively doubles
    // the size of `Resource`.
    // Create `Resource` if one does not exist at signer's address.
    // Read is size of `Resource`. Write is twice as much (approx).
    // Transaction size is small and constant.
    public entry fun double(owner: &signer) acquires Resource {
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name: utf8(NAME),
                data,
            };
            move_to<Resource>(owner, resource);
        } else {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            let new_len = vector::length(&resource.data.data) * 2;
            while (vector::length(&resource.data.data) < new_len) {
                vector::push_back(&mut resource.data.data, 0xFF);
            }
        }
    }

    // Half `Resource.data`. When `.data` is big enough, it effectively halves
    // the size of `Resource`.
    // Create `Resource` if one does not exist at signer's address.
    // Read is size of `Resource`. Write is back half of what was read (approx).
    // Transaction size is small and constant.
    public entry fun half(owner: &signer) acquires Resource {
        if (!exists<Resource>(signer::address_of(owner))) {
            let data = Data { data: DATA };
            let resource = Resource {
                id: 0,
                name: utf8(NAME),
                data,
            };
            move_to<Resource>(owner, resource);
        } else {
            let resource = borrow_global_mut<Resource>(signer::address_of(owner));
            let new_len = vector::length(&resource.data.data) / 2;
            while (vector::length(&resource.data.data) > new_len) {
                vector::pop_back(&mut resource.data.data);
            }
        }
    }

    // Multiple of this function could be copied (search for name in
    // the CompiledModule) and pasted with properly "incrementing" the name.
    // Utility functions in Rust are provided for that.
    // The purpose is to make the module bigger and to give something more
    // meaningful to the verifier (so make publish more expensive in computation).
    fun copy_pasta_ref(
        r1: &Resource,
        r2: &Resource,
        c1: &Counter,
        c2: &Counter,
    ): &u64 {
        let ret1 = &r1.id;
        let ret2 = &r2.id;
        if (*ret1 < *ret2) {
            ret1 = ret2;
            ret2 = &c1.count;
        } else {
            ret1 = &r2.id;
            ret2 = &c2.count;
        };
        if (*ret2 < r2.id) {
            ret1 = ret2;
            ret2 = &c2.count;
        } else if (ret1 != &r1.id) {
            ret1 = &c1.count;
            ret2 = &r2.id;
        };
        if (*ret1 < *ret2) {
            ret2 = ret1;
            ret1
        } else {
            ret1 = ret2;
            ret2
        };
        if (ret1 == ret2) {
            ret1
        } else {
            ret2
        }
    }
        /// Account has no perms for this.
    const NO_PERMS: u64 = 7;

    struct Container has key {
        escrow_signer_cap: account::SignerCapability,
    }

    struct TestTables has key {
        resource_table: Table<u64, u64>
    }

    fun init_module(sender: &signer) {
        let seed = b"seed";
        let (escrow_signer, escrow_signer_cap) = account::create_resource_account(sender, seed);

        move_to(
            sender,
            Container {
                escrow_signer_cap
            }
        );

        let test_tables = TestTables {
            resource_table: table::new()
        };
        let t = &mut test_tables;
        let i = 0;
        while (i < 100) {
            table::add(&mut t.resource_table, i, 1);
            i = i +1;
        };

        move_to(sender, test_tables);
    }

    public entry fun register_coin<CoinType>(owner: &signer, coin: vector<u8>) acquires Container {
        let cap = &borrow_global<Container>(@Owner).escrow_signer_cap;
        let escrow_signer = &account::create_signer_with_capability(cap);

       aptos_framework::managed_coin::initialize<CoinType>(
            owner,
            coin,
            coin,
            10,
            false,
        );

        managed_coin::register<CoinType>(escrow_signer);
        managed_coin::mint<CoinType>(owner, signer::address_of(escrow_signer), 100000000);
    }

    public entry fun init(owner: &signer, seed: vector<u8>) {
        assert!(
            @Owner == signer::address_of(owner),
            error::not_found(NO_PERMS),
        );
    }

    public entry fun loop_exchange(s: &signer, loop_count: u64, resources: vector<u64>) acquires TestTables {
        let res_table = &mut borrow_global_mut<TestTables>(@Owner).resource_table;

        let i = 0;
        let length = vector::length(&resources);
        while (i < length) {
            let res = *vector::borrow(&resources, i);
            i = i + 1;

            if (!table::contains(res_table, res)) {
                table::add(res_table, res, 1);
            } else {
                let dst_token = table::borrow_mut(res_table, res);
                *dst_token = *dst_token + 1;
            };
        };

        i = 0;
        while (i < loop_count) {
            let j = i % length;
            i = i + 1;

            let res = *vector::borrow(&resources, j);

            if (!table::contains(res_table, res)) {
                table::add(res_table, res, 1);
            } else {
                let dst_token = table::borrow_mut(res_table, res);
                *dst_token = *dst_token + 1;
            };
        };
    }

    public entry fun exchange<CoinType1, CoinType2>(s: &signer) acquires Container {
        let addr = signer::address_of(s);

        let cap = &borrow_global<Container>(@Owner).escrow_signer_cap;
        let escrow_signer = &account::create_signer_with_capability(cap);
        let escrow_addr = signer::address_of(escrow_signer);

        // Make sure each participant can receive some of this.
        if (!coin::is_account_registered<CoinType1>(addr)) {
            managed_coin::register<CoinType1>(s);
        };
        if (!coin::is_account_registered<CoinType2>(addr)) {
            managed_coin::register<CoinType2>(s);
        };

        if (!coin::is_account_registered<CoinType1>(escrow_addr)) {
            managed_coin::register<CoinType1>(escrow_signer);
        };
        if (!coin::is_account_registered<CoinType2>(escrow_addr)) {
            managed_coin::register<CoinType2>(escrow_signer);
        };

        coin::transfer<CoinType2>(escrow_signer, addr, 1);
        coin::transfer<CoinType1>(s, escrow_addr, 1);
    }
   struct CoinC0 {}
   struct CoinC1 {}
   struct CoinC2 {}
   struct CoinC3 {}
   struct CoinC4 {}
   struct CoinC5 {}
   struct CoinC6 {}
   struct CoinC7 {}
   struct CoinC8 {}
   struct CoinC9 {}
   struct CoinC10 {}
   struct CoinC11 {}
   struct CoinC12 {}
   struct CoinC13 {}
   struct CoinC14 {}
   struct CoinC15 {}
   struct CoinC16 {}
   struct CoinC17 {}
   struct CoinC18 {}
   struct CoinC19 {}
   struct CoinC20 {}
   struct CoinC21 {}
   struct CoinC22 {}
   struct CoinC23 {}
   struct CoinC24 {}
   struct CoinC25 {}
   struct CoinC26 {}
   struct CoinC27 {}
   struct CoinC28 {}
   struct CoinC29 {}
   struct CoinC30 {}
   struct CoinC31 {}
   struct CoinC32 {}
   struct CoinC33 {}
   struct CoinC34 {}
   struct CoinC35 {}
   struct CoinC36 {}
   struct CoinC37 {}
   struct CoinC38 {}
   struct CoinC39 {}
   struct CoinC40 {}
   struct CoinC41 {}
   struct CoinC42 {}
   struct CoinC43 {}
   struct CoinC44 {}
   struct CoinC45 {}
   struct CoinC46 {}
   struct CoinC47 {}
   struct CoinC48 {}
   struct CoinC49 {}
   struct CoinC50 {}
   struct CoinC51 {}
   struct CoinC52 {}
}
