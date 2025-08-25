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
    let perPage = $state(localStorage.getItem('perPage') ? parseInt(localStorage.getItem('perPage')) : 100)
    let pages = $derived(Math.ceil(messages / perPage))
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
    }, () => [id, page, perPage])

    $effect(() => {
        localStorage.setItem('perPage', perPage.toString())
    })

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
                let p = new Promise(resolve => setTimeout(resolve, delay))
                await p
                p.then(() => loadMessages(delay * 1.2))
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
        height: 1.8em;
    }
</style>

<SearchForm {query}
            {fromDate}
            {toDate}/>

<div class="w-full px-4">

    <Pagination bind:page
                bind:perPage
                {pages}>
        <div class="py-2 flex items-center gap-2">
            {#if messages}
                {messages} messages
            {/if}
            {#if !finished}
                <Loader2 class="animate-spin"/>
            {/if}
        </div>
    </Pagination>

    <div class="py-4">

        {#each pageMessages as message, i}
            <div class="message"
                 id="msg_{i}">
                <div class="flex flex-row">
                    <div class="min-w-4 text-gray-400 w-4"
                         title="Expand">
                        <div
                                class="hide_icon cursor-pointer"
                                onclick={(e) => {
                                   document.querySelector(`#msg_${i} .message_content`).classList.toggle('truncate');
                                }}
                        >
                            <ArrowDownFromLine class="w-3"/>
                        </div>
                    </div>
                    <div class="message_content truncate p-1 w-full text-sm whitespace-pre-wrap overflow-x-hidden"
                         class:bg-gray-100={i % 2 === 1}>
                        <pre class="whitespace-pre-wrap">{message}</pre>
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

