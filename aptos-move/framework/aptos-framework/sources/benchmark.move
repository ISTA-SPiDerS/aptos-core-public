module aptos_framework::benchmark {
    use aptos_framework::account;
    use std::signer;
    use aptos_framework::coin;
    use aptos_framework::managed_coin;
    use std::error;
    use std::vector;
    use std::table::{Self, Table};

    friend aptos_framework::genesis;

    /// Account has no perms for this.
    const NO_PERMS: u64 = 7;

    struct TestTables has key {
        resource_table: Table<u64, u64>
    }

    public(friend) fun init(sender: &signer) {
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

    public entry fun loop_exchange(s: &signer, loop_count: u64, resources: vector<u64>) acquires TestTables {
        let res_table = &mut borrow_global_mut<TestTables>(@aptos_framework).resource_table;

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

    public entry fun exchange(s: &signer, resource: u64) acquires TestTables {
       let res_table = &mut borrow_global_mut<TestTables>(@aptos_framework).resource_table;
       if (!table::contains(res_table, resource)) {
           table::add(res_table, resource, 1);
       } else {
           let dst_token = table::borrow_mut(res_table, resource);
           *dst_token = *dst_token + 1;
       };
    }
}
