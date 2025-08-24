<script>
    import {ArrowLeft, ArrowRight} from "lucide-svelte";

    let {
        pages = $bindable(1),
        page = $bindable(1),
        perPage = $bindable(0),
        children = () => ""
    } = $props()


    const pageSizes = [100, 500, 1000, 5000]

    let prevPageExists = $derived(page > 1)
    let nextPageExists = $derived(page < pages)
</script>


<div class="flex items-center">

    <nav class="flex items-center -space-x-px pr-2">
        <button type="button"
                disabled={!prevPageExists}
                onclick={() => page--}
                class="cursor-pointer py-2 px-2 inline-flex first:rounded-s-lg last:rounded-e-lg border border-gray-200  justify-center items-center gap-x-1.5   text-gray-800  disabled:opacity-50 disabled:pointer-events-none">
            <ArrowLeft/>
            <span class="">Previous</span>
        </button>
        {#if pages > 0}
            <div class="flex items-center ">
                <div class="border border-gray-200 py-2 px-3 flex items-center">Page</div>
                <select bind:value={page}
                        class="flex h-[42px] justify-center items-center border-t border-b border-gray-200  text-gray-800 py-2 px-3 focus:outline-none">
                    {#each Array(pages) as _,i}
                        <option value={i + 1}>{i + 1}</option>
                    {/each}
                </select>
                <div class=" border border-gray-200 py-2 px-3 flex items-center">of {pages}</div>
            </div>
        {/if}
        {#if perPage > 0}
            <div class="flex items-center">
                <div class="border border-gray-200 py-2 px-3 flex items-center">Show</div>
                <select bind:value={perPage}
                        class="flex h-[42px] justify-center items-center border-t border-b border-gray-200 text-gray-800 py-2 px-3 focus:outline-none">
                    {#each pageSizes as size}
                        <option value={size}>{size}</option>
                    {/each}
                </select>
                <div class="border border-gray-200 py-2 px-3 flex items-center">items</div>
            </div>
        {/if}
        <button type="button"
                disabled={!nextPageExists}
                onclick={() => page++}
                class="cursor-pointer py-2 px-2 inline-flex justify-center items-center gap-x-1.5  first:rounded-s-lg last:rounded-e-lg border border-gray-200 text-gray-800  disabled:opacity-50 disabled:pointer-events-none"
        >
            <span class="">Next</span>
            <ArrowRight size={24}/>
        </button>
    </nav>

    {@render children()}
</div>
