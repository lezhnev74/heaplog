<script>
    import SearchForm from "../lib/SearchForm.svelte";
    import {onMount} from "svelte";

    let isLoading = $state(true);
    let queries = $state([])

    async function fetchQueries() {
        try {
            const response = await fetch('/api/query');
            if (!response.ok) throw new Error('Failed to fetch queries');
            let j = await response.json();
            queries = j.queries
        } catch (error) {
            console.error('Error fetching queries:', error);
            queries = [];
        } finally {
            isLoading = false
        }
    }

    onMount(() => {
        fetchQueries();
    });

</script>


<SearchForm/>
<div class="w-full px-4 py-4">
    {#if isLoading}
        Loading...
    {:else}
        <div class="space-y-4">
            <div>Recent Queries</div>
            {#each queries as query}
                <div class="py-4 rounded-lg cursor-pointer flex">
                    <div class="w-1/5 bg-gray-100">
                        <code class="p-4">{query.Query}</code>
                    </div>
                    <div class="flex-grow text-gray-400 pl-4">
                    {#if query.FromDate}
                            <div>From: &nbsp;{new Date(query.FromDate).toLocaleString()}</div>
                        {/if}
                        {#if query.ToDate}
                            <div>To: &nbsp;&nbsp;&nbsp;{new Date(query.ToDate).toLocaleString()}</div>
                        {/if}
                    </div>
                </div>
            {:else}
                <div>No queries.</div>
            {/each}
        </div>
    {/if}
</div>

