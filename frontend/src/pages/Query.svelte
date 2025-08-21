<script>
    import SearchForm from "../lib/SearchForm.svelte";
    import Pagination from "../lib/Pagination.svelte";

    let {
        query = "",
        fromDate = 0,
        toDate = 0,
        finished = false,
        messages = 10,
    } = $props()

    let perPage = 100
    let pages = $derived(messages / perPage + 100)
    let page = $state(1)

    $effect(() => console.log('new page', page))

</script>

<SearchForm {query}
            {fromDate}
            {toDate}/>

<div class="w-full px-4">

    {#if pages}
        <Pagination bind:page
                    {pages}
                    showStatus={true}
                    {finished}
                    {messages}/>
    {/if}

    <div class="py-4">
        Loading messages...
    </div>

    {#if pages}
        <Pagination bind:page
                    {pages}/>
    {/if}
</div>

