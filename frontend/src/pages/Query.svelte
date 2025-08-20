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

<main>
    Random 2 value is {random}
    <button onclick={fetchRandom}>Refresh</button>
</main>

