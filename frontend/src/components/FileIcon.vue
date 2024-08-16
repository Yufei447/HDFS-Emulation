<template>
    <div class="fileIcon">
        <div v-if="isFile">
            <img src="../assets/file.png" alt="file">
        </div>
        <div v-else>
            <img src="../assets/folder.png" alt="folder">
        </div>
        <div>
            {{fileName}}
        </div>
    </div>
</template>

<script>
export default {
    name: "FileIcon",
    props: {
        isFile: Boolean,
        fileName: String,
    }
}
</script>

<style scoped>
.fileIcon {
    margin-bottom: 25px;
    width: 65px;
    height: 85px;
}

</style>