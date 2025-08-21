<script>
    import {ArrowLeft, ArrowRight} from "lucide-svelte";

    let {
        pages = 20,
        page = 2,
        finished = false,
        pageSize = 100,
    } = $props()

    let prevPageExists = $derived(page > 1)
    let nextPageExists = $derived(page < pages)
</script>


<nav class="flex items-center -space-x-px">
    {#if prevPageExists}
        <button type="button"
                class="cursor-pointer py-2 px-2 inline-flex justify-center items-center gap-x-1.5  first:rounded-s-lg last:rounded-e-lg border border-gray-200 text-gray-800  disabled:opacity-50 disabled:pointer-events-none">
            <ArrowLeft/>
            <span class="">Previous</span>
        </button>
    {/if}
    <div class="flex items-center ">
        <div class=" border border-gray-200 py-2 px-3 flex items-center">Page</div>
        <select value={page}
                class="flex h-[42px] justify-center items-center text-gray-800 py-2 px-3 border-b border-t border-gray-200 focus:outline-none">
            {#each Array(pages) as _,i}
                <option value={i + 1}>{i + 1}</option>
            {/each}
        </select>
        <div class=" border border-gray-200 py-2 px-3 flex items-center">of {pages}</div>
    </div>
    {#if nextPageExists}
        <button type="button"
                class="cursor-pointer py-2 px-2 inline-flex justify-center items-center gap-x-1.5  first:rounded-s-lg last:rounded-e-lg border border-gray-200 text-gray-800  disabled:opacity-50 disabled:pointer-events-none"
        >
            <span class="">Next</span>
            <ArrowRight size={24}/>
        </button>
    {/if}
</nav>