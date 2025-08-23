<script>
    import SearchForm from "../lib/SearchForm.svelte";
    import Pagination from "../lib/Pagination.svelte";
    import {ArrowDownFromLine, Loader2} from "lucide-svelte";
    import {untrack} from "svelte";
    import {ExplicitEffect} from "../lib/lib.svelte.js";

    let {
        id,
        query = "",
        fromDate = 0,
        toDate = 0,
        finished = false,
        messages = 0,
    } = $props()

    let page = $state(1)
    let perPage = $state(5)
    let pages = $derived(Math.floor(messages / perPage))
    let pageMessages = $state([])
    let truncatedMessages = $state([])
    // derived: page is incomplete if not enough messages yet
    let pageComplete = $derived(
        pageMessages.length >= perPage ||
        (finished && ((messages - (page - 1) * perPage) === pageMessages.length))
    )
    let currentController = null;

    $effect(() => {
        // detect truncated messages
        if (pageMessages.length) {
            detectTruncated()
        }
    })

    ExplicitEffect(() => {
        // reset on new id for the query
        page = 1
    }, () => [id])
    ExplicitEffect(() => {
        // on next/prev page initiation
        pageMessages = []
        untrack(() => pageMessages)
        loadMessages()
    }, () => [id, page])


    // Load messages loads the remaining messages from the server to fill up the current page.
    async function loadMessages(delay = 1000) {
        // cancel previous fetch
        if (currentController) {
            currentController.abort();
        }
        currentController = new AbortController();
        
        try {
            const skip = (page - 1) * perPage + pageMessages.length
            const limit = perPage - pageMessages.length
            const response = await fetch(`/api/query/` + id + `?skip=${skip}&limit=${limit}`)
            const data = await response.json()
            pageMessages = [...pageMessages, ...data.messages]
            finished = data.query.finished
            messages = data.query.messages

            if (!pageComplete || !finished) {
                let p = new Promise(resolve => setTimeout(resolve, delay * 1.1))
                await p
                p.then(() => loadMessages(delay))
            }
        } catch (error) {
            console.error('Failed to fetch messages:', error)
        }
    }

    function detectTruncated() {
        const truncated = [];
        Array.from(document.querySelectorAll('.message_content')).forEach((element, index) => {
            if (element.scrollWidth > element.clientWidth || element.scrollHeight > element.clientHeight) {
                truncated.push(index);
            }
        });
        truncatedMessages = truncated;
    }

</script>

<style>
    .truncate {
        overflow: hidden;
        display: -webkit-box;
        -webkit-line-clamp: 1;
        -webkit-box-orient: vertical;
    }
</style>

<SearchForm {query}
            {fromDate}
            {toDate}/>

<div class="w-full px-4">

    <Pagination bind:page
                {pages}>
        <div class="py-2 flex items-center gap-2">
            {#if messages}
                {messages} messages
            {/if}
            {#if !pageComplete}
                <Loader2 class="animate-spin"/>
            {/if}
        </div>
    </Pagination>

    <div class="py-4">

        {#each pageMessages as message, i}
            <div class="message"
                 id="msg_{i}">
                <div class="flex flex-row">
                    <div class="min-w-4 text-gray-400 w-4 cursor-pointer" title="Expand">
                        <div
                                class="hide_icon"
                                class:hidden={!truncatedMessages.includes(i)}
                                onclick={(e) => {
                                   document.querySelector(`#msg_${i} .message_content`).classList.remove('truncate');
                                   e.currentTarget.closest('.hide_icon').classList.add('hidden');
                                }}
                        >
                            <ArrowDownFromLine class="w-3"/>
                        </div>
                    </div>
                    <div class="message_content truncate py-1 w-full text-sm"
                         class:bg-gray-100={i % 2 === 1}>
                        <pre>{message}</pre>
                    </div>
                </div>
            </div>
        {:else}
            {#if !pageComplete}
                Loading messages...
            {:else}
                Nothing found.
            {/if}
        {/each}
    </div>

    {#if pages}
        <Pagination bind:page
                    {pages}/>
    {/if}
</div>

