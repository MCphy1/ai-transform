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
                <div class="item-detail-time">创建: {{ getDateStr(r.create_at) }}</div>
                <div class="item-detail-failed">失败: {{ r.failed_at ? getDateStr(r.failed_at) : '无' }}</div>
                <div class="item-detail-retry">重试: {{ r.retry_count }}次</div>
                <el-tooltip :content="r.error_message" placement="top" :disabled="!r.error_message">
                    <div class="item-detail-error">原因: {{ truncateError(r.error_message) }}</div>
                </el-tooltip>
            </div>

            <el-button type="primary" class="item-btn" @click="retryRecord(r)" :loading="r.retrying">
                重新处理
            </el-button>
        </el-card>

        <el-empty v-if="!loading && recordList.length === 0" description="暂无失败任务" />
    </div>
    </div>
</template>

<script lang="ts" setup>
import { reactive, ref, onBeforeMount } from "vue"
import { ElNotification } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { failedRecord, getFailedRecords, retryFailedRecord } from "../api/api.ts"
import { getDateStr } from "../utils/utils.ts"

interface FailedRecordWithRetry extends failedRecord {
    retrying?: boolean
}

const loading = ref(false)
let recordList = reactive([] as FailedRecordWithRetry[])

onBeforeMount(() => {
    loadRecords()
})

function loadRecords() {
    loading.value = true
    getFailedRecords().then(function (res) {
        recordList.splice(0)
        for (let a of res.data) {
            recordList.push({
                id: a.id,
                project_name: a.project_name,
                original_language: a.original_language,
                translated_language: a.translated_language,
                original_video_url: a.original_video_url,
                error_message: a.error_message,
                failed_at: a.failed_at,
                retry_count: a.retry_count,
                create_at: a.create_at,
                retrying: false
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

function retryRecord(record: FailedRecordWithRetry) {
    record.retrying = true
    retryFailedRecord(record.id).then(() => {
        ElNotification({
            title: 'Success',
            message: '重试任务已提交',
            type: 'success',
        })
        const index = recordList.findIndex(r => r.id === record.id)
        if (index > -1) {
            recordList.splice(index, 1)
        }
    }).catch((err) => {
        ElNotification({
            title: 'Error',
            message: err.message || '重试失败',
            type: 'error',
        })
    }).finally(() => {
        record.retrying = false
    })
}

function truncateError(message: string | undefined): string {
    if (!message) return '无'
    if (message.length > 20) {
        return message.substring(0, 20) + '...'
    }
    return message
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
    height: 18rem;
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
    border: 2px solid rgb(244, 67, 54);
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
    background-color: rgb(244, 67, 54);
    position: absolute;
    border-bottom-right-radius: 1.25rem;
    border-bottom-left-radius: 1.25rem;
    top: 11.325rem;
    left: 0;
}
.item-detail {
    width: 22rem;
    height: 6rem;
    left: 0;
    top: 11.625rem;
    position: absolute;
    padding: 0.5rem;
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
.item-detail-failed {
    position: absolute;
    left: 0.5rem;
    top: 3.2rem;
    color: #f56c6c;
    font-size: 12px;
}
.item-detail-retry {
    position: absolute;
    left: 0.5rem;
    top: 4.2rem;
    color: #e6a23c;
    font-size: 12px;
}
.item-detail-error {
    position: absolute;
    left: 0.5rem;
    top: 5.2rem;
    color: #f56c6c;
    font-size: 12px;
    max-width: 21rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
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
