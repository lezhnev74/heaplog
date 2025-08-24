<script>
    import Home from "./pages/Home.svelte";
    import Query from "./pages/Query.svelte";
    import Footer from "./lib/Footer.svelte";
    import NotFound from "./pages/NotFound.svelte";
    import {page, renderPage} from "./lib/navigation.svelte.js";

    // registry of page components
    const components = {
        Home,
        Query,
        NotFound
    };

    const routes = {
        '/': 'Home',
        '/query/:id': 'Query',
        '': NotFound,
    }

    let Current = $derived(components[page.component]);
    let props = $derived(page.props);

    $effect(() => {
        if (window.__INITIAL_PAGE__) {
            renderPage(window.__INITIAL_PAGE__, window.location.pathname);
            window.__INITIAL_PAGE__ = null;
        }
    });

    // Handle browser back/forward
    window.onpopstate = (e) => {
        page.component = e.state.component;
        page.props = e.state.props;
    };
</script>

<main class="min-h-screen w-full flex flex-col">
    <div class="flex-grow w-full">
        <Current {...props}/>
    </div>
    <Footer/>
</main>
