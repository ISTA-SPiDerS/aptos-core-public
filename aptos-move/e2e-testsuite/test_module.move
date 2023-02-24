module Owner::Exchange {
    use aptos_framework::account;
    use std::signer;
    use aptos_framework::coin;
    use aptos_framework::managed_coin;
    use std::error;

    /// Account has no perms for this.
    const NO_PERMS: u64 = 7;

    struct Container has key {
        escrow_signer_cap: account::SignerCapability,
    }

    fun init_module(sender: &signer) {
        let seed = b"seed";
        let (escrow_signer, escrow_signer_cap) = account::create_resource_account(sender, seed);

        aptos_framework::managed_coin::initialize<CoinC>(
            sender,
            b"Coin_C",
            b"Cc",
            10,
            false,
        );

        managed_coin::register<CoinC>(&escrow_signer);
        managed_coin::mint<CoinC>(sender, signer::address_of(&escrow_signer), 1000000);

        move_to(
            sender,
            Container {
                escrow_signer_cap
            }
        );
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
        if (coin::balance<CoinType1>(addr) < 1) {
            coin::transfer<CoinType1>(escrow_signer, addr, 100);
        };

        coin::transfer<CoinType1>(s, escrow_addr, 1);
        coin::transfer<CoinType2>(escrow_signer, addr, 1);
    }
    // Struct CoinC1 {}
    // Struct CoinC2 {}
    // Struct CoinC3 {}
    // Struct CoinC4 {}
    // Struct CoinC5 {}
    // Struct CoinC6 {}
    // Struct CoinC7 {}
    // Struct CoinC8 {}
    // Struct CoinC9 {}
    // Struct CoinC10 {}
    // Struct CoinC11 {}
    // Struct CoinC12 {}
    // Struct CoinC13 {}
    // Struct CoinC14 {}
    // Struct CoinC15 {}
    // Struct CoinC16 {}
    // Struct CoinC17 {}
    // Struct CoinC18 {}
    // Struct CoinC19 {}
    // Struct CoinC20 {}
    // Struct CoinC21 {}
    // Struct CoinC22 {}
    // Struct CoinC23 {}
    // Struct CoinC24 {}
    // Struct CoinC25 {}
    // Struct CoinC26 {}
    // Struct CoinC27 {}
    // Struct CoinC28 {}
    // Struct CoinC29 {}
    // Struct CoinC30 {}
    // Struct CoinC31 {}
    // Struct CoinC32 {}
    // Struct CoinC33 {}
    // Struct CoinC34 {}
    // Struct CoinC35 {}
    // Struct CoinC36 {}
    // Struct CoinC37 {}
    // Struct CoinC38 {}
    // Struct CoinC39 {}
    // Struct CoinC40 {}
    // Struct CoinC41 {}
    // Struct CoinC42 {}
    // Struct CoinC43 {}
    // Struct CoinC44 {}
    // Struct CoinC45 {}
    // Struct CoinC46 {}
    // Struct CoinC47 {}
    // Struct CoinC48 {}
    // Struct CoinC49 {}
    // Struct CoinC50 {}
    // Struct CoinC51 {}
    // Struct CoinC52 {}
    // Struct CoinC53 {}
    // Struct CoinC54 {}
    // Struct CoinC55 {}
    // Struct CoinC56 {}
    // Struct CoinC57 {}
    // Struct CoinC58 {}
    // Struct CoinC59 {}
    // Struct CoinC60 {}
    // Struct CoinC61 {}
    // Struct CoinC62 {}
    // Struct CoinC63 {}
    // Struct CoinC64 {}
    // Struct CoinC65 {}
    // Struct CoinC66 {}
    // Struct CoinC67 {}
    // Struct CoinC68 {}
    // Struct CoinC69 {}
    // Struct CoinC70 {}
    // Struct CoinC71 {}
    // Struct CoinC72 {}
    // Struct CoinC73 {}
    // Struct CoinC74 {}
    // Struct CoinC75 {}
    // Struct CoinC76 {}
    // Struct CoinC77 {}
    // Struct CoinC78 {}
    // Struct CoinC79 {}
    // Struct CoinC80 {}
    // Struct CoinC81 {}
    // Struct CoinC82 {}
    // Struct CoinC83 {}
    // Struct CoinC84 {}
    // Struct CoinC85 {}
    // Struct CoinC86 {}
    // Struct CoinC87 {}
    // Struct CoinC88 {}
    // Struct CoinC89 {}
    // Struct CoinC90 {}
    // Struct CoinC91 {}
    // Struct CoinC92 {}
    // Struct CoinC93 {}
    // Struct CoinC94 {}
    // Struct CoinC95 {}
    // Struct CoinC96 {}
    // Struct CoinC97 {}
    // Struct CoinC98 {}
    // Struct CoinC99 {}
    // Struct CoinC100 {}

}
