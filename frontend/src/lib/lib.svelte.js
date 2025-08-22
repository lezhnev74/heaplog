import {untrack} from "svelte";

export function ExplicitEffect(fn, depsFn) {
    // https://github.com/sveltejs/svelte/issues/9248
    $effect(() => {
        depsFn();
        untrack(fn);
    });
}