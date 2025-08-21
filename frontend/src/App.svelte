<script>
    import Home from "./pages/Home.svelte";
    import Query from "./pages/Query.svelte";
    import Footer from "./lib/Footer.svelte";
    import NotFound from "./pages/NotFound.svelte";
    import {page, renderPage} from "./lib/navigation.svelte.js";
    import {compileRoute, matchRoute} from "./lib/router-utils.js";

    // registry of page components
    const components = {
        Home,
        Query,
        NotFound
    };

    const routes = {
        '/': 'Home',
        '/query/:Id': 'Query',
        '': NotFound,
    }
    const routes2 = Object.entries(routes).map(([path, component]) => {
        const {regex, keys} = compileRoute(path);
        return {path, component, regex, keys};
    });

    let Current = $derived(components[page.component]);
    let props = $derived(page.props);

    $effect(() => console.log(page))

    $effect(() => {
        if (window.__INITIAL_PAGE__) {
            renderPage(window.__INITIAL_PAGE__, window.location.pathname);
        }
        // for (const route of routes2) {
        //     const params = matchRoute(route, window.location.pathname);
        //     if (params) {
        //         console.log(route, params)
        //         renderPage({component: route.component, props: params}, window.location.pathname);
        //         break;
        //     }
        // }
    });

    // // Handle browser back/forward
    // $effect(() => {
    //     function handler() {
    //         renderPage(window.location.pathname);
    //     }
    //     window.addEventListener("popstate", handler);
    //     return () => window.removeEventListener("popstate", handler);
    // });
</script>

<main class="min-h-screen w-full flex flex-col">
    <div class="flex-grow w-full">
        <Current {...props}/>
    </div>
    <Footer/>
</main>
