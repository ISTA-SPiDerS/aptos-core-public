module Owner::Exchange {
    use aptos_framework::account;
    use std::signer;
    use aptos_framework::coin;
    use aptos_framework::managed_coin;
    use std::error;
    use std::vector;

    /// Account has no perms for this.
    const NO_PERMS: u64 = 7;

    struct Container has key {
        escrow_signer_cap: account::SignerCapability,
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

        // Make sure each participant got coins to exchange
        // if (coin::balance<CoinType1>(addr) < 10) {
            coin::transfer<CoinType1>(escrow_signer, addr, 100);
        // };

        coin::transfer<CoinType2>(escrow_signer, addr, 1);
        // let b: u64 = 10000;
        // let i = 0;
        // let l = vector::pop_back(&mut account::get_authentication_key(addr));

        // while (i < l % 5) {
        //     b = b * 2;
        //     let a = b;
        //     while (a > 0) {
        //         a = a - 1;
        //     };
        //     i = i + 1;
        // };
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
    struct CoinC53 {}
    struct CoinC54 {}
    struct CoinC55 {}
    struct CoinC56 {}
    struct CoinC57 {}
    struct CoinC58 {}
    struct CoinC59 {}
    struct CoinC60 {}
    struct CoinC61 {}
    struct CoinC62 {}
    struct CoinC63 {}
    struct CoinC64 {}
    struct CoinC65 {}
    struct CoinC66 {}
    struct CoinC67 {}
    struct CoinC68 {}
    struct CoinC69 {}
    struct CoinC70 {}
    struct CoinC71 {}
    struct CoinC72 {}
    struct CoinC73 {}
    struct CoinC74 {}
    struct CoinC75 {}
    struct CoinC76 {}
    struct CoinC77 {}
    struct CoinC78 {}
    struct CoinC79 {}
    struct CoinC80 {}
    struct CoinC81 {}
    struct CoinC82 {}
    struct CoinC83 {}
    struct CoinC84 {}
    struct CoinC85 {}
    struct CoinC86 {}
    struct CoinC87 {}
    struct CoinC88 {}
    struct CoinC89 {}
    struct CoinC90 {}
    struct CoinC91 {}
    struct CoinC92 {}
    struct CoinC93 {}
    struct CoinC94 {}
    struct CoinC95 {}
    struct CoinC96 {}
    struct CoinC97 {}
    struct CoinC98 {}
    struct CoinC99 {}
    struct CoinC100 {}
}
