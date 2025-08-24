<script>
    import SearchForm from "../lib/SearchForm.svelte";

    let {queries = []} = $props()

</script>


<SearchForm/>

<div class="w-full px-4 py-4">
    <div class="space-y-4">
        {#if queries.length > 0}
            <div>Recent Queries</div>
            {#each queries as query}
                <div onclick={() => window.location.href = '/query/' + query.id}
                     class="py-4 rounded-lg cursor-pointer flex">
                    <div class="w-1/5 bg-gray-100">
                        <code class="p-4">{query.query}</code>
                    </div>
                    <div class="w-1/5 text-gray-400 pl-4">
                        {#if query.fromDate}
                            <div>From: &nbsp;{new Date(query.fromDate).toLocaleString()}</div>
                        {/if}
                        {#if query.toDate}
                            <div>To: &nbsp;&nbsp;&nbsp;{new Date(query.toDate).toLocaleString()}</div>
                        {/if}
                    </div>
                    <div class="flex-grow text-gray-400 pr-4">
                        {query.messages} messages
                    </div>
                </div>
            {/each}
        {:else}
            <div>No recent queries.</div>
        {/if}
    </div>
</div>

