<script>
    import {onMount} from 'svelte';

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


Random value is {random}
<button onclick={fetchRandom}>Refresh</button>


