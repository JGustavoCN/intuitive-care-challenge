<template>
  <v-app>
    <Navbar />

    <v-main class="bg-grey-lighten-4">
      <v-container>
        <router-view />
      </v-container>
    </v-main>
  </v-app>
</template>

<script setup>
import Navbar from "./components/Navbar.vue";
</script>
