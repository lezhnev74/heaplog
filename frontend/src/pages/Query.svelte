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
    let page = 1
    let pages = messages / perPage

</script>

<SearchForm {query}
            {fromDate}
            {toDate}/>

<div class="w-full px-4">

    <Pagination/>

    <div class="py-4">
        Search is in progress...
    </div>

    {#if messages > 0}
        <Pagination/>
    {/if}
</div>

