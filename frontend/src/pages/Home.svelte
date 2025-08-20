<script>
    import {onMount} from 'svelte';
    import SearchForm from "../lib/SearchForm.svelte";

    let random = $state(0);

    async function fetchRandom() {
        const response = await fetch('/api/random');
        const data = await response.json();
        random = data.random;
    }

    onMount(() => {
        fetchRandom();
    });
</script>


<SearchForm/>
<div class="w-full px-4">
    Random value is {random}
    <button onclick={fetchRandom}>Refresh</button>
</div>

