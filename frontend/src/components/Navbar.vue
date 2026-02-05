<template>
  <v-app-bar color="primary" density="compact">
    <v-app-bar-title>Intuitive Care</v-app-bar-title>

    <template v-slot:append>
      <v-btn to="/" variant="text">Dashboard</v-btn>
      <v-btn to="/operadoras" variant="text">Operadoras</v-btn>
    </template>
  </v-app-bar>
</template>

<script setup>
// Nenhuma lógica necessária aqui, o Vuetify cuida dos links com a prop 'to'
</script>
