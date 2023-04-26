
<a name="0x1_benchmark"></a>

# Module `0x1::benchmark`



-  [Resource `TestTables`](#0x1_benchmark_TestTables)
-  [Constants](#@Constants_0)
-  [Function `init`](#0x1_benchmark_init)
-  [Function `loop_exchange`](#0x1_benchmark_loop_exchange)
-  [Function `exchange`](#0x1_benchmark_exchange)


<pre><code><b>use</b> <a href="../../aptos-stdlib/doc/table.md#0x1_table">0x1::table</a>;
</code></pre>



<a name="0x1_benchmark_TestTables"></a>

## Resource `TestTables`



<pre><code><b>struct</b> <a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a> <b>has</b> key
</code></pre>



<details>
<summary>Fields</summary>


<dl>
<dt>
<code>resource_table: <a href="../../aptos-stdlib/doc/table.md#0x1_table_Table">table::Table</a>&lt;u64, u64&gt;</code>
</dt>
<dd>

</dd>
</dl>


</details>

<a name="@Constants_0"></a>

## Constants


<a name="0x1_benchmark_NO_PERMS"></a>

Account has no perms for this.


<pre><code><b>const</b> <a href="benchmark.md#0x1_benchmark_NO_PERMS">NO_PERMS</a>: u64 = 7;
</code></pre>



<a name="0x1_benchmark_init"></a>

## Function `init`



<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="benchmark.md#0x1_benchmark_init">init</a>(sender: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b>(<b>friend</b>) <b>fun</b> <a href="benchmark.md#0x1_benchmark_init">init</a>(sender: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>) {
    <b>let</b> test_tables = <a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a> {
        resource_table: <a href="../../aptos-stdlib/doc/table.md#0x1_table_new">table::new</a>()
    };
    <b>let</b> t = &<b>mut</b> test_tables;
    <b>let</b> i = 0;
    <b>while</b> (i &lt; 100) {
        <a href="../../aptos-stdlib/doc/table.md#0x1_table_add">table::add</a>(&<b>mut</b> t.resource_table, i, 1);
        i = i +1;
    };

    <b>move_to</b>(sender, test_tables);
}
</code></pre>



</details>

<a name="0x1_benchmark_loop_exchange"></a>

## Function `loop_exchange`



<pre><code><b>public</b> entry <b>fun</b> <a href="benchmark.md#0x1_benchmark_loop_exchange">loop_exchange</a>(s: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, loop_count: u64, resources: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u64&gt;)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> entry <b>fun</b> <a href="benchmark.md#0x1_benchmark_loop_exchange">loop_exchange</a>(s: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, loop_count: u64, resources: <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector">vector</a>&lt;u64&gt;) <b>acquires</b> <a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a> {
    <b>let</b> res_table = &<b>mut</b> <b>borrow_global_mut</b>&lt;<a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a>&gt;(@aptos_framework).resource_table;

    <b>let</b> i = 0;
    <b>let</b> length = <a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector_length">vector::length</a>(&resources);
    <b>while</b> (i &lt; length) {
        <b>let</b> res = *<a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector_borrow">vector::borrow</a>(&resources, i);
        i = i + 1;

        <b>if</b> (!<a href="../../aptos-stdlib/doc/table.md#0x1_table_contains">table::contains</a>(res_table, res)) {
            <a href="../../aptos-stdlib/doc/table.md#0x1_table_add">table::add</a>(res_table, res, 1);
        } <b>else</b> {
            <b>let</b> dst_token = <a href="../../aptos-stdlib/doc/table.md#0x1_table_borrow_mut">table::borrow_mut</a>(res_table, res);
            *dst_token = *dst_token + 1;
        };
    };

    i = 0;
    <b>while</b> (i &lt; loop_count) {
        <b>let</b> j = i % length;
        i = i + 1;

        <b>let</b> res = *<a href="../../aptos-stdlib/../move-stdlib/doc/vector.md#0x1_vector_borrow">vector::borrow</a>(&resources, j);

        <b>if</b> (!<a href="../../aptos-stdlib/doc/table.md#0x1_table_contains">table::contains</a>(res_table, res)) {
            <a href="../../aptos-stdlib/doc/table.md#0x1_table_add">table::add</a>(res_table, res, 1);
        } <b>else</b> {
            <b>let</b> dst_token = <a href="../../aptos-stdlib/doc/table.md#0x1_table_borrow_mut">table::borrow_mut</a>(res_table, res);
            *dst_token = *dst_token + 1;
        };
    };
}
</code></pre>



</details>

<a name="0x1_benchmark_exchange"></a>

## Function `exchange`



<pre><code><b>public</b> entry <b>fun</b> <a href="benchmark.md#0x1_benchmark_exchange">exchange</a>(s: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, resource: u64)
</code></pre>



<details>
<summary>Implementation</summary>


<pre><code><b>public</b> entry <b>fun</b> <a href="benchmark.md#0x1_benchmark_exchange">exchange</a>(s: &<a href="../../aptos-stdlib/../move-stdlib/doc/signer.md#0x1_signer">signer</a>, resource: u64) <b>acquires</b> <a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a> {
   <b>let</b> res_table = &<b>mut</b> <b>borrow_global_mut</b>&lt;<a href="benchmark.md#0x1_benchmark_TestTables">TestTables</a>&gt;(@aptos_framework).resource_table;
   <b>if</b> (!<a href="../../aptos-stdlib/doc/table.md#0x1_table_contains">table::contains</a>(res_table, resource)) {
       <a href="../../aptos-stdlib/doc/table.md#0x1_table_add">table::add</a>(res_table, resource, 1);
   } <b>else</b> {
       <b>let</b> dst_token = <a href="../../aptos-stdlib/doc/table.md#0x1_table_borrow_mut">table::borrow_mut</a>(res_table, resource);
       *dst_token = *dst_token + 1;
   };
}
</code></pre>



</details>


[move-book]: https://move-language.github.io/move/introduction.html
