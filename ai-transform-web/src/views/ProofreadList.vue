<template>
    <div class="page-container">
        <div class="nav-header">
            <router-link to="/" class="nav-link" active-class="active">翻译任务</router-link>
            <router-link to="/proofread" class="nav-link" active-class="active">字幕校对</router-link>
            <router-link to="/deadletter" class="nav-link" active-class="active">失败任务</router-link>
            <el-button type="primary" :icon="Refresh" @click="loadRecords" :loading="loading" class="refresh-btn">
                刷新
            </el-button>
        </div>
        <div v-loading="loading" class="loading">

        <el-card v-for="r in recordList" :key="r.id" class="item">
            <video :src="r.original_video_url" class="item-video" controls></video>
            <span class="item-video-footer"></span>

            <div class="item-detail">
                <div class="item-detail-name">{{ r.project_name }}</div>
                <div class="item-detail-lang">{{ r.original_language }} -> {{ r.translated_language }}</div>
                <div class="item-detail-time">{{ getDateStr(r.create_at) }}</div>
            </div>

            <el-button type="primary" class="item-btn" @click="goToProofread(r.id)">
                开始校对
            </el-button>
        </el-card>

        <el-empty v-if="!loading && recordList.length === 0" description="暂无待校对任务" />
    </div>
    </div>
</template>

<script lang="ts" setup>
import { reactive, ref, onBeforeMount } from "vue"
import { useRouter } from "vue-router"
import { ElNotification } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { proofreadRecord, getPendingProofreads } from "../api/api.ts"
import { getDateStr } from "../utils/utils.ts"

const router = useRouter()
const loading = ref(false)
let recordList = reactive([] as proofreadRecord[])

onBeforeMount(() => {
    loadRecords()
})

function loadRecords() {
    loading.value = true
    getPendingProofreads().then(function (res) {
        recordList.splice(0)
        for (let a of res.data) {
            recordList.push({
                id: a.id,
                project_name: a.project_name,
                original_language: a.original_language,
                translated_language: a.translated_language,
                original_video_url: a.original_video_url,
                status: a.status,
                translated_srt_for_proofread_url: a.translated_srt_for_proofread_url,
                create_at: a.create_at,
            })
        }
    }).catch((err) => {
        ElNotification({
            title: 'Error',
            message: err.message || '获取失败',
            type: 'error',
        })
    }).finally(() => {
        loading.value = false
    })
}

function goToProofread(id: number) {
    router.push(`/proofread/${id}`)
}
</script>

<style scoped>
.page-container {
    width: 100%;
    height: 100%;
}
.nav-header {
    display: flex;
    gap: 20px;
    padding: 15px 20px;
    background-color: #f5f5f5;
    border-bottom: 1px solid #ddd;
}
.nav-link {
    text-decoration: none;
    color: #666;
    font-size: 16px;
    padding: 8px 16px;
    border-radius: 4px;
}
.nav-link:hover {
    background-color: #e0e0e0;
}
.nav-link.active {
    color: #409eff;
    background-color: #e6f7ff;
}
.refresh-btn {
    margin-left: auto;
}
.loading {
    position: relative;
    display: flex;
    white-space: normal;
    word-wrap: break-word;
    width: 100%;
    height: 100%;
    flex-wrap: wrap;
    align-content: first baseline;
    justify-content: first baseline;
}
.item {
    width: 22rem;
    height: 15.75rem;
    border-radius: 1.25rem;
    margin-right: 0.5rem;
    margin-bottom: 1rem;
    padding: 0;
    position: relative;
    font-family: Inter;
    display: flex;
    align-items: center;
    justify-content: center;
}
.item:hover {
    border: 2px solid rgb(15, 7, 243);
}
.item-video {
    width: 100%;
    height: 11.625rem;
    border-radius: 1.25rem;
    position: absolute;
    top: 0;
    left: 0;
}
.item-video-footer {
    width: 100%;
    height: 0.3rem;
    background-color: rgb(15, 7, 243);
    position: absolute;
    border-bottom-right-radius: 1.25rem;
    border-bottom-left-radius: 1.25rem;
    top: 11.325rem;
    left: 0;
}
.item-detail {
    width: 22rem;
    height: 4.125rem;
    left: 0;
    top: 11.625rem;
    position: absolute;
}
.item-detail-name {
    font-weight: bold;
    position: absolute;
    left: 0.5rem;
    top: 0.2rem;
    font-size: 14px;
}
.item-detail-lang {
    position: absolute;
    left: 0.5rem;
    top: 1.2rem;
    color: #666;
    font-size: 12px;
}
.item-detail-time {
    position: absolute;
    left: 0.5rem;
    top: 2.2rem;
    color: #999;
    font-size: 12px;
}
.item-btn {
    position: absolute;
    right: 0.5rem;
    bottom: 0.5rem;
}
:deep(.el-card__body) {
    padding: 0;
    align-content: center;
}
</style>
