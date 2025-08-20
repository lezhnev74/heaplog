<script>
    import Home from "./pages/Home.svelte";
    import Query from "./pages/Query.svelte";
    import Footer from "./lib/Footer.svelte";
    import NotFound from "./pages/NotFound.svelte";
    import { page, renderPage } from "./lib/navigation.svelte.js";

    // registry of page components
    const components = {
        Home,
        Query,
        NotFound
    };

    let Current = $derived(components[page.component]);
    let props = $derived(page.props);

    $effect(() => {
        console.log(page, Current);
    })

    // Handle browser back/forward
    $effect(() => {
        function handler() {
            renderPage(window.location.pathname, true);
        }
        window.addEventListener("popstate", handler);
        return () => window.removeEventListener("popstate", handler);
    });
</script>

<main class="min-h-screen w-full flex flex-col">
    <div class="flex-grow w-full">
        <Current {...props}/>
    </div>
    <Footer/>
</main>
