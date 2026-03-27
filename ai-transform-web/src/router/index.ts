import { createRouter, createWebHistory } from 'vue-router'
import TranslateRecords from '../views/TranslateRecords.vue'
import ProofreadList from '../views/ProofreadList.vue'
import ProofreadEditor from '../views/ProofreadEditor.vue'
import DeadLetterList from '../views/DeadLetterList.vue'

const routes = [
    {
        path: '/',
        name: 'TranslateRecords',
        component: TranslateRecords
    },
    {
        path: '/proofread',
        name: 'ProofreadList',
        component: ProofreadList
    },
    {
        path: '/proofread/:id',
        name: 'ProofreadEditor',
        component: ProofreadEditor
    },
    {
        path: '/deadletter',
        name: 'DeadLetterList',
        component: DeadLetterList
    },
]

const router = createRouter({
    history: createWebHistory(),
    routes
})
export default router