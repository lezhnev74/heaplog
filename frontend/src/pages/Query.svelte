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
        if (pageMessages.length) {
            detectTruncated()
        }
    })

    ExplicitEffect(() => {
        page = 1
    }, () => [id])
    ExplicitEffect(() => {
        // page reload
        pageMessages = []
        untrack(() => pageMessages)
        loadMessages()
    }, () => [id, page])


    // Load messages loads the remaining messages from the server to fill up the current page.
    async function loadMessages() {
        // cancel previous fetch
        if (currentController) {
            currentController.abort();
        }
        currentController = new AbortController();

        try {
            const skip = (page - 1) * perPage + pageMessages.length
            const limit = perPage - pageMessages.length
            if (limit <= 0) return;
            console.log('loadMessages', skip, limit)
            const response = await fetch(`/api/query/` + id + `?skip=${skip}&limit=${limit}`)
            const data = await response.json()
            pageMessages = [...pageMessages, ...data.messages]
            finished = data.query.finished
            messages = data.query.messages

            if (!pageComplete) {
                let p = new Promise(resolve => setTimeout(resolve, 1_000))
                await p
                p.then(() => loadMessages())
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
                    <div class="min-w-4 py-1 text-gray-400 w-4 cursor-pointer">
                        <div
                                class="hide_icon"
                                class:hidden={!truncatedMessages.includes(i)}
                                onclick={(e) => {
                                   document.querySelector(`#msg_${i} .message_content`).classList.remove('truncate');
                                   e.currentTarget.closest('.hide_icon').classList.add('hidden');
                                }}
                        >
                            <ArrowDownFromLine class="w-4 cursor-pointer"/>
                        </div>
                    </div>
                    <div class="message_content truncate py-1"
                         class:bg-gray-100={i % 2 === 1}>
                        {message}
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

