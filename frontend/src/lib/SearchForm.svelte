<script>
    import {Calendar} from "lucide-svelte";
    import {onMount} from 'svelte';
    import flatpickr from 'flatpickr';
    import {renderPage} from "./navigation.svelte.js";

    let {
        query = "err",
        from = 1755706024333,
        to = "",
    } = $props()

    let isLoading = $state(false);
    let searchError = $state("");
    let fromInput;
    let toInput;
    let fromPicker;
    let toPicker;

    async function handleSubmit(event) {
        event.preventDefault();
        isLoading = true;
        let body = {
            query: query,
            from: fromPicker.selectedDates.length ? fromPicker.formatDate(fromPicker.selectedDates[0], "Z") : "",
            to: toPicker.selectedDates.length ? toPicker.formatDate(toPicker.selectedDates[0], "Z") : ""
        }
        console.log(body)
        const response = await fetch('/api/query/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(body)
        });

        const data = await response.json();
        if (response.ok) {
            await renderPage(data, "/query/" + data.props.id)
        } else {
            searchError = data.error ?? "bad request";
        }
        isLoading = false;
    }


    onMount(() => {
        const conf = {
            enableTime: true,
            dateFormat: "Y-m-d H:i:S",
            allowInput: true,
            allowInvalidPreload: true,
            clickOpens: false
        }
        fromPicker = flatpickr(fromInput, {...conf, defaultDate: from || null});
        toPicker = flatpickr(toInput, {...conf, defaultDate: to || null});
        return () => {
            fromPicker && fromPicker.destroy();
            toPicker && toPicker.destroy();
        };
    });
</script>


<div class="container px-4">
    <form autocomplete="off"
          onsubmit={handleSubmit}>
        <div class="flex flex-wrap gap-4 my-2.5">
            <div class="w-1/4">
                <textarea class="w-full p-2.5 border border-gray-300 rounded-lg outline-none h-[calc(100%-1px)]"
                          name="Text"
                          placeholder="Type in your query here..."
                          spellcheck="false"
                          style="min-height: 150px;"
                          bind:value={query}
                >{query}</textarea>
            </div>
            <div class="w-1/4">
                <div class="flex"
                     id="query_from"
                >
                    <span class="inline-flex items-center px-3 w-[60px] text-gray-900 bg-gray-200 border border-r-0 border-gray-300 rounded-l-lg">From</span>
                    <input
                            id="query_from_input"
                            class="rounded-none bg-gray-50 border text-gray-900 block outline-none flex-1 min-w-0 w-full border-gray-300 p-2.5"
                            type="text"
                            bind:this={fromInput}
                    />
                    <span
                            class="inline-flex items-center px-3 text-gray-900 bg-gray-200 border border-l-0 border-gray-300 rounded-r-lg cursor-pointer"
                            onclick={() => fromPicker && fromPicker.open()}
                    >
                       <i class=""><Calendar class="w-5 h-5"
                                             aria-label="Calendar"/></i>
                     </span>
                </div>
                <div class="flex my-2.5"
                     id="query_to"
                >
                    <span class="inline-flex items-center px-3 w-[60px] text-gray-900 bg-gray-200 border border-r-0 border-gray-300 rounded-l-lg">To</span>
                    <input
                            type="text"
                            id="query_to_input"
                            class="rounded-none bg-gray-50 border text-gray-900 outline-none block flex-1 min-w-0 w-full border-gray-300 p-2.5"
                            bind:this={toInput}
                    />
                    <span
                            class="inline-flex items-center px-3 text-gray-900 bg-gray-200 border border-l-0 border-gray-300 rounded-r-lg cursor-pointer"
                            onclick={() => toPicker && toPicker.open()}
                    >
                       <i class=""><Calendar class="w-5 h-5"
                                             aria-label="Calendar"/></i>
                     </span>
                </div>
                <div class="input-group">
                    <div class="flex items-center gap-2">
                        <input type="submit"
                               value={isLoading ? "Searching..." : "Search"}
                               disabled={isLoading}
                               class="bg-[#fff83b] font-bold py-2.5 px-4 rounded-lg cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"/>
                        <span>{searchError}</span>
                    </div>
                </div>
            </div>
        </div>
    </form>
</div>