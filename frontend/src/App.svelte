<script>
    import Router from "svelte-spa-router";
    import Home from "./pages/Home.svelte";
    import Query from "./pages/Query.svelte";
    import Footer from "./lib/Footer.svelte";
    import NotFound from "./pages/NotFound.svelte";

    const routes = {
        "/": Home,
        "/query/:id": Query,
        "*": NotFound,
    };
</script>

<main class="min-h-screen w-full flex flex-col">
    <div class="flex-grow w-full">
        <Router {routes}/>
    </div>
    <Footer/>
</main>
