<script>
    import SearchForm from "../lib/SearchForm.svelte";

    let {
        query = "",
        fromDate = 0,
        toDate = 0,
    } = $props()
</script>

<SearchForm {query} {fromDate} {toDate}/>
<div class="w-full px-4">
    Search is in progress...
</div>

