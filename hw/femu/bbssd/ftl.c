#include "ftl.h"

// #define FEMU_DEBUG_FTL

uint64_t io_count = 0;
static uint64_t pages_moved;
static uint64_t pages_written_by_host;
static struct ssd *g_ssd = NULL;
static pthread_t tid;
static uint64_t hot_data_threshold = 10000000; // All data is cold by default

static void *ftl_thread(void *arg);

static void print_statistics(int signum);
static void setup_timer(void);
static int compare(const void *a, const void *b);
static void trim_newline(char *str);

static int compare(const void *a, const void *b)
{
    uint64_t num1 = *(uint64_t *)a;
    uint64_t num2 = *(uint64_t *)b;

    return num1 - num2;
}

static void trim_newline(char *str)
{
    size_t len = strlen(str);
    while (len > 0 && (str[len - 1] == '\n' || str[len - 1] == '\r' || isspace(str[len - 1])))
    {
        str[len - 1] = '\0';
        --len;
    }
}

static void *fifo_handler(void *arg)
{
    const char *fifo_name = "femu_fifo";
    char buffer[256];

    if (mkfifo(fifo_name, 0666) == -1 && errno != EEXIST) // Creates a FIFO
    {
        ftl_log("mkfifo failed\n");
        pthread_exit(NULL);
    }
    if (chmod(fifo_name, 0666) == -1) // Changes the permissions of the FIFO
    {
        ftl_log("chmod failed\n");
        pthread_exit(NULL);
    }
    int fd = open(fifo_name, O_RDONLY | O_NONBLOCK); // Opens the FIFO
    if (fd == -1)
    {
        ftl_log("Failed to open fifo\n");
        pthread_exit(NULL);
    }
    ftl_log("Clearing existing FIFO contents...\n");

    int bytes = read(fd, buffer, sizeof(buffer) - 1);
    if (bytes > 0)
    {
        do // Reads and discards the contents of the FIFO
        {
            buffer[bytes] = '\0';
            printf("Discarded: %s\n", buffer);
            bytes = read(fd, buffer, sizeof(buffer) - 1);
        } while (bytes > 0);
    }
    else if (bytes == -1 && errno != EAGAIN)
    {
        ftl_log("Error reading from FIFO\n");
        close(fd);
        pthread_exit(NULL);
    }
    close(fd);

    fd = open(fifo_name, O_RDONLY); // Opens the FIFO
    if (fd == -1)
    {
        ftl_log("Failed to open fifo\n");
        pthread_exit(NULL);
    }

    while (true)
    {
        bytes = read(fd, buffer, sizeof(buffer) - 1); // Reads from the FIFO
        if (bytes > 0)
        {
            buffer[bytes] = '\0';
            trim_newline(buffer); // Trims the newline character

            char *cmd = strtok(buffer, " "); // Tokenizes the buffer by whitespace
            if (cmd == NULL)
                continue;

            if (!strcmp(cmd, "clear"))
            {
                if (g_ssd != NULL)
                {
                    for (int i = 0; i < g_ssd->sp.tt_pgs; ++i)
                        g_ssd->page_write_counts[i] = 0; // Clears the page write counts
                    ftl_log("Cleared page_write_counts\n");
                }
            }
            else if (!strcmp(cmd, "save"))
            {
                FILE *file = fopen("result.txt", "w");
                if (file == NULL)
                {
                    ftl_log("Failed to open result.txt\n");
                    pthread_exit(NULL);
                }

                fprintf(file, "hot data threshold: %lu\n", hot_data_threshold);

                qsort(g_ssd->page_write_counts, g_ssd->sp.tt_pgs, sizeof(uint64_t), compare); // Sorts the page write counts

                for (int i = 0; i < g_ssd->sp.tt_pgs; ++i)
                {
                    if (g_ssd->page_write_counts[i] != 0)
                    {
                        fprintf(file, "%lu,\n", g_ssd->page_write_counts[i]); // Writes the page write counts to the file
                    }
                }
                ftl_log("Stats saved\n");
                fclose(file);
            }
            else if (!strcmp(cmd, "threshold"))
            {
                char *value_str = strtok(NULL, " ");
                if (value_str != NULL)
                {
                    hot_data_threshold = (uint64_t)atoi(value_str); // Sets the hot data threshold
                    ftl_log("Set hot data threshold to %lu\n", hot_data_threshold);
                }
                else
                {
                    ftl_log("Error: Missing threshold value\n");
                }
            }
            else
                ftl_log("Unknown FIFO command: %s\n", cmd);
            fflush(stdout);
        }
    }
    close(fd);
    pthread_exit(NULL);
}

static void print_statistics(int signum)
{
    if (io_count == 0)
        return;
    time_t now;
    struct tm *timeinfo;
    char time_str[20];

    time(&now);
    timeinfo = localtime(&now);

    strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", timeinfo);

    double waf = pages_written_by_host == 0 ? 0 : (double)(pages_written_by_host + pages_moved) / pages_written_by_host;
    femu_log("[%s] IOPS: %lu, WAF: %.2f\n", time_str, io_count, waf); // Prints the IOPS and WAF
    pages_moved = 0;
    pages_written_by_host = 0;
    io_count = 0;
}

static void setup_timer(void)
{
    struct sigaction sa;
    struct itimerval timer;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = &print_statistics; // Sets the signal handler
    sigaction(SIGALRM, &sa, NULL);

    timer.it_value.tv_sec = 1; // Sets the timer to 1 second
    timer.it_value.tv_usec = 0;
    timer.it_interval.tv_sec = 10; // Sets the interval to 10 second
    timer.it_interval.tv_usec = 0;

    setitimer(ITIMER_REAL, &timer, NULL); // Sets the timer
}

static inline bool is_hot_data(struct ssd *ssd, uint64_t lpn)
{
    return ssd->page_write_counts[lpn] > hot_data_threshold;
}

static inline bool should_gc(struct ssd *ssd)
{
    // Checks if the free line count is less than or equal to the GC threshold(default is 25%)
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines);
}

static inline bool should_gc_high(struct ssd *ssd)
{
    // Checks if the free line count is less than or equal to the high GC threshold(default is 5%)
    return (ssd->lm.free_line_cnt <= ssd->sp.gc_thres_lines_high);
}

static inline struct ppa get_maptbl_ent(struct ssd *ssd, uint64_t lpn)
{
    return ssd->maptbl[lpn];
}

static inline void set_maptbl_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa)
{
    ftl_assert(lpn < ssd->sp.tt_pgs);
    ssd->maptbl[lpn] = *ppa;
}

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch * spp->pgs_per_ch +
            ppa->g.lun * spp->pgs_per_lun +
            ppa->g.pl * spp->pgs_per_pl +
            ppa->g.blk * spp->pgs_per_blk +
            ppa->g.pg;

    ftl_assert(pgidx < spp->tt_pgs);

    return pgidx;
}

static inline uint64_t get_rmap_ent(struct ssd *ssd, struct ppa *ppa)
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct ssd *ssd, uint64_t lpn, struct ppa *ppa) // Sets the LPN for the given PPA in the reverse mapping table
{
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = lpn;
}

static inline int victim_line_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_line_get_pri(void *a)
{
    return ((struct line *)a)->vpc;
}

static inline void victim_line_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct line *)a)->vpc = pri;
}

static inline size_t victim_line_get_pos(void *a)
{
    return ((struct line *)a)->pos;
}

static inline void victim_line_set_pos(void *a, size_t pos)
{
    ((struct line *)a)->pos = pos;
}

static void ssd_init_lines(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;
    struct line_mgmt *lm = &ssd->lm;
    struct line *line;

    lm->tt_lines = spp->blks_per_pl;
    ftl_assert(lm->tt_lines == spp->tt_lines);
    lm->lines = g_malloc0(sizeof(struct line) * lm->tt_lines);

    QTAILQ_INIT(&lm->free_line_list);
    lm->victim_line_pq = pqueue_init(spp->tt_lines, victim_line_cmp_pri,
                                     victim_line_get_pri, victim_line_set_pri,
                                     victim_line_get_pos, victim_line_set_pos);
    QTAILQ_INIT(&lm->full_line_list);

    lm->free_line_cnt = 0;
    for (int i = 0; i < lm->tt_lines; i++)
    {
        line = &lm->lines[i];
        line->id = i;
        line->ipc = 0;
        line->vpc = 0;
        line->pos = 0;
        /* initialize all the lines as free lines */
        QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
        lm->free_line_cnt++;
    }

    ftl_assert(lm->free_line_cnt == lm->tt_lines);
    lm->victim_line_cnt = 0;
    lm->full_line_cnt = 0;
}

#ifdef HOT_COLD_DATA_SEPARATION
static void ssd_init_write_pointer(struct ssd *ssd, bool is_hot)
#else
static void ssd_init_write_pointer(struct ssd *ssd)
#endif
{
#ifdef HOT_COLD_DATA_SEPARATION
    struct write_pointer *wpp = is_hot ? &ssd->hot_wp : &ssd->cold_wp;
#else
    struct write_pointer *wpp = &ssd->wp;
#endif
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;

    /* wpp->curline is always our next-to-write super-block */

    wpp->curline = curline;
    wpp->ch = 0;
    wpp->lun = 0;
    wpp->pg = 0;
    wpp->blk = curline->id; // Sets the block index to the ID of the current line
    wpp->pl = 0;
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static struct line *get_next_free_line(struct ssd *ssd)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *curline = NULL;

    curline = QTAILQ_FIRST(&lm->free_line_list);
    if (!curline)
    {
        ftl_err("No free lines left in [%s] !!!!\n", ssd->ssdname);
        return NULL;
    }

    QTAILQ_REMOVE(&lm->free_line_list, curline, entry);
    lm->free_line_cnt--;
    return curline;
}

#ifdef HOT_COLD_DATA_SEPARATION
static void ssd_advance_write_pointer(struct ssd *ssd, bool is_hot)
#else
static void ssd_advance_write_pointer(struct ssd *ssd)
#endif
{
    struct ssdparams *spp = &ssd->sp;
#ifdef HOT_COLD_DATA_SEPARATION
    struct write_pointer *wpp = is_hot ? &ssd->hot_wp : &ssd->cold_wp;
#else
    struct write_pointer *wpp = &ssd->wp;
#endif

    struct line_mgmt *lm = &ssd->lm;

    check_addr(wpp->ch, spp->nchs); // Checks if the channel index is within the valid range
    wpp->ch++;                      // Increments the channel index
    if (wpp->ch == spp->nchs)       // If the channel index is out of range
    {
        wpp->ch = 0;                            // Resets the channel index
        check_addr(wpp->lun, spp->luns_per_ch); // Checks if the LUN index is within the valid range
        wpp->lun++;                             // Increments the LUN index
        /* in this case, we should go to next lun */
        if (wpp->lun == spp->luns_per_ch) // If the LUN index is out of range
        {
            wpp->lun = 0; // Resets the LUN index
            /* go to next page in the block */
            check_addr(wpp->pg, spp->pgs_per_blk); // Checks if the page index is within the valid range
            wpp->pg++;                             // Increments the page index
            if (wpp->pg == spp->pgs_per_blk)       // If the page index is out of range
            {
                wpp->pg = 0; // Resets the page index
                /* move current line to {victim,full} line list */
                if (wpp->curline->vpc == spp->pgs_per_line) // If all pages in the current line are valid
                {
                    /* all pgs are still valid, move to full line list */
                    ftl_assert(wpp->curline->ipc == 0);
                    QTAILQ_INSERT_TAIL(&lm->full_line_list, wpp->curline, entry); // Inserts the current line at the tail of full_line_list
                    lm->full_line_cnt++;                                          // Increments the full line count
                }
                else
                {
                    ftl_assert(wpp->curline->vpc >= 0 && wpp->curline->vpc < spp->pgs_per_line);
                    /* there must be some invalid pages in this line */
                    ftl_assert(wpp->curline->ipc > 0);
                    pqueue_insert(lm->victim_line_pq, wpp->curline); // Inserts the current line into victim_line_pq
                    lm->victim_line_cnt++;                           // Increments the victim line count
                }
                /* current line is used up, pick another empty line */
                check_addr(wpp->blk, spp->blks_per_pl); // Checks if the block index is within the valid range
                wpp->curline = NULL;
                wpp->curline = get_next_free_line(ssd); // Sets the current line to the next free line
                if (!wpp->curline)
                {
                    /* TODO */
                    abort();
                }
                wpp->blk = wpp->curline->id;            // Sets the block index to the ID of the current line
                check_addr(wpp->blk, spp->blks_per_pl); // Checks if the block index is within the valid range
                /* make sure we are starting from page 0 in the super block */
                ftl_assert(wpp->pg == 0);
                ftl_assert(wpp->lun == 0);
                ftl_assert(wpp->ch == 0);
                /* TODO: assume # of pl_per_lun is 1, fix later */
                ftl_assert(wpp->pl == 0);
            }
        }
    }
}

#ifdef HOT_COLD_DATA_SEPARATION
static struct ppa get_new_page(struct ssd *ssd, bool is_hot)
#else
static struct ppa get_new_page(struct ssd *ssd)
#endif
{
#ifdef HOT_COLD_DATA_SEPARATION
    struct write_pointer *wpp = is_hot ? &ssd->hot_wp : &ssd->cold_wp;
#else
    struct write_pointer *wpp = &ssd->wp;
#endif
    struct ppa ppa;
    ppa.ppa = 0;
    ppa.g.ch = wpp->ch;
    ppa.g.lun = wpp->lun;
    ppa.g.pg = wpp->pg;
    ppa.g.blk = wpp->blk;
    ppa.g.pl = wpp->pl;
    ftl_assert(ppa.g.pl == 0);

    return ppa;
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    // ftl_assert(is_power_of_2(spp->luns_per_ch));
    // ftl_assert(is_power_of_2(spp->nchs));
}

static void ssd_init_params(struct ssdparams *spp, FemuCtrl *n)
{
    spp->secsz = n->bb_params.secsz;             // 512
    spp->secs_per_pg = n->bb_params.secs_per_pg; // 8
    spp->pgs_per_blk = n->bb_params.pgs_per_blk; // 256
    spp->blks_per_pl = n->bb_params.blks_per_pl; /* 256 16GB */
    spp->pls_per_lun = n->bb_params.pls_per_lun; // 1
    spp->luns_per_ch = n->bb_params.luns_per_ch; // 8
    spp->nchs = n->bb_params.nchs;               // 8

    spp->pg_rd_lat = n->bb_params.pg_rd_lat;
    spp->pg_wr_lat = n->bb_params.pg_wr_lat;
    spp->blk_er_lat = n->bb_params.blk_er_lat;
    spp->ch_xfer_lat = n->bb_params.ch_xfer_lat;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch = spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    /* line is special, put it at the end */
    spp->blks_per_line = spp->tt_luns; /* TODO: to fix under multiplanes */
    spp->pgs_per_line = spp->blks_per_line * spp->pgs_per_blk;
    spp->secs_per_line = spp->pgs_per_line * spp->secs_per_pg;
    spp->tt_lines = spp->blks_per_lun; /* TODO: to fix under multiplanes */

    spp->gc_thres_pcent = n->bb_params.gc_thres_pcent / 100.0;
    spp->gc_thres_lines = (int)((1 - spp->gc_thres_pcent) * spp->tt_lines);
    spp->gc_thres_pcent_high = n->bb_params.gc_thres_pcent_high / 100.0;
    spp->gc_thres_lines_high = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_lines);
    spp->enable_gc_delay = true;

    check_params(spp);
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++)
    {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++)
    {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);
    for (int i = 0; i < pl->nblks; i++)
    {
        ssd_init_nand_blk(&pl->blk[i], spp);
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++)
    {
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++)
    {
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++)
    {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++)
    {
        ssd->rmap[i] = INVALID_LPN;
    }
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(spp, n);

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++)
    {
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize all the lines */
    ssd_init_lines(ssd);

    /* initialize write pointer, this is how we allocate new pages for writes */
#ifdef HOT_COLD_DATA_SEPARATION
    ssd_init_write_pointer(ssd, true);  // Initializes the write pointer for hot data
    ssd_init_write_pointer(ssd, false); // Initializes the write pointer for cold data
#else
    ssd_init_write_pointer(ssd); // Initializes the write pointer
#endif

    ssd->page_write_counts = g_malloc0(sizeof(uint64_t) * spp->tt_pgs); // Allocates memory for the page write counts

    setup_timer(); // Sets up the timer

    g_ssd = ssd;

    if (pthread_create(&tid, NULL, fifo_handler, NULL) != 0) // Creates a FIFO handler thread
    {
        ftl_log("Failed to create thread\n");
        exit(1);
    }

    qemu_thread_create(&ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n,
                       QEMU_THREAD_JOINABLE); // Creates ftl_thread
}

static inline bool valid_ppa(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch && pl >= 0 && pl < spp->pls_per_lun && blk >= 0 && blk < spp->blks_per_pl && pg >= 0 && pg < spp->pgs_per_blk && sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct ssd *ssd, uint64_t lpn)
{
    return (lpn < ssd->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct line *get_line(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->lm.lines[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    switch (c)
    {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime)
                         ? cmd_stime
                         : lun->next_lun_avail_time;            // Sets the start time
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat; // Sets the next available time for the LUN
        lat = lun->next_lun_avail_time - cmd_stime;             // Calculates the latency
#if 0                                                           // This path is not taken
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;

        /* read: then data transfer through channel */
        chnl_stime = (ch->next_ch_avail_time < lun->next_lun_avail_time) ? \
            lun->next_lun_avail_time : ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        lat = ch->next_ch_avail_time - cmd_stime;
#endif
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime)
                         ? cmd_stime
                         : lun->next_lun_avail_time; // Sets the start time
        if (ncmd->type == USER_IO)
        {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat; // Sets the next available time for the LUN
        }
        else
        {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat; // Sets the next available time for the LUN
        }
        lat = lun->next_lun_avail_time - cmd_stime; // Calculates the latency

#if 0
        chnl_stime = (ch->next_ch_avail_time < cmd_stime) ? cmd_stime : \
                     ch->next_ch_avail_time;
        ch->next_ch_avail_time = chnl_stime + spp->ch_xfer_lat;

        /* write: then do NAND program */
        nand_stime = (lun->next_lun_avail_time < ch->next_ch_avail_time) ? \
            ch->next_ch_avail_time : lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;

        lat = lun->next_lun_avail_time - cmd_stime;
#endif
        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime)
                         ? cmd_stime
                         : lun->next_lun_avail_time;             // Sets the start time
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat; // Sets the next available time for the LUN
        lat = lun->next_lun_avail_time - cmd_stime;              // Calculates the latency
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat; // Returns the latency
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    bool was_full_line = false;
    struct line *line;

    /* update corresponding page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->ipc >= 0 && blk->ipc < spp->pgs_per_blk);
    blk->ipc++;
    ftl_assert(blk->vpc > 0 && blk->vpc <= spp->pgs_per_blk);
    blk->vpc--;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    ftl_assert(line->ipc >= 0 && line->ipc < spp->pgs_per_line);
    if (line->vpc == spp->pgs_per_line)
    {
        ftl_assert(line->ipc == 0);
        was_full_line = true;
    }
    line->ipc++;
    ftl_assert(line->vpc > 0 && line->vpc <= spp->pgs_per_line);
    /* Adjust the position of the victime line in the pq under over-writes */
    if (line->pos)
    {
        /* Note that line->vpc will be updated by this call */
        pqueue_change_priority(lm->victim_line_pq, line->vpc - 1, line);
    }
    else
    {
        line->vpc--;
    }

    if (was_full_line)
    {
        /* move line: "full" -> "victim" */
        QTAILQ_REMOVE(&lm->full_line_list, line, entry);
        lm->full_line_cnt--;
        pqueue_insert(lm->victim_line_pq, line);
        lm->victim_line_cnt++;
    }
}

static void mark_page_valid(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;
    struct line *line;

    /* update page status */
    pg = get_pg(ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ssd->sp.pgs_per_blk);
    blk->vpc++;

    /* update corresponding line status */
    line = get_line(ssd, ppa);
    ftl_assert(line->vpc >= 0 && line->vpc < ssd->sp.pgs_per_line);
    line->vpc++;
}

static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++)
    {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    ftl_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt++;
}

static void gc_read_page(struct ssd *ssd, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ssd->sp.enable_gc_delay) // If GC delay is enabled (default is true)
    {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ssd, ppa, &gcr); // Updates the status of the SSD
    }
}

/* move valid page data (already in DRAM) from victim line to a new page */
static uint64_t gc_write_page(struct ssd *ssd, struct ppa *old_ppa)
{
    struct ppa new_ppa;
    struct nand_lun *new_lun;

    uint64_t lpn = get_rmap_ent(ssd, old_ppa); // Gets the LPN using the PPA from the reverse mapping table

    ftl_assert(valid_lpn(ssd, lpn));

#ifdef HOT_COLD_DATA_SEPARATION
    bool is_hot = is_hot_data(ssd, lpn); // Checks if the data is hot or cold
    new_ppa = get_new_page(ssd, is_hot); // Gets a new page
#else
    new_ppa = get_new_page(ssd); // Gets a new page
#endif
    /* update maptbl */
    set_maptbl_ent(ssd, lpn, &new_ppa); // Sets LPN->PPA mapping in the mapping table
    /* update rmap */
    set_rmap_ent(ssd, lpn, &new_ppa); // Sets PPA->LPN mapping in the reverse mapping table

    mark_page_valid(ssd, &new_ppa); // Marks the page as valid

    /* need to advance the write pointer here */
#ifdef HOT_COLD_DATA_SEPARATION
    ssd_advance_write_pointer(ssd, is_hot); // Advances the write pointer
#else
    ssd_advance_write_pointer(ssd); // Advances the write pointer
#endif

    if (ssd->sp.enable_gc_delay) // If GC delay is enabled (default is true)
    {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ssd, &new_ppa, &gcw); // Updates the status of the SSD
    }

    /* advance per-ch gc_endtime as well */
#if 0 // This path is not taken
    new_ch = get_ch(ssd, &new_ppa);
    new_ch->gc_endtime = new_ch->next_ch_avail_time;
#endif

    new_lun = get_lun(ssd, &new_ppa);                   // Returns the pointer to the LUN
    new_lun->gc_endtime = new_lun->next_lun_avail_time; // Updates gc_endtime of the LUN

    return 0;
}

static struct line *select_victim_line(struct ssd *ssd, bool force)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *victim_line = NULL;

    victim_line = pqueue_peek(lm->victim_line_pq); // Gets the line with the lowest valid page count from the priority queue
    if (!victim_line)
    {
        return NULL;
    }

    // If GC is not forced and the invalid page count in the victim line is less than 1/8 of the total pages in the line
    if (!force && victim_line->ipc < ssd->sp.pgs_per_line / 8)
    {
        return NULL;
    }

    pqueue_pop(lm->victim_line_pq); // Pops the victim line from the queue
    victim_line->pos = 0;           // Resets pos of the victim line used in victim_line_pq
    lm->victim_line_cnt--;

    /* victim_line is a danggling node now */
    return victim_line;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < spp->pgs_per_blk; pg++) // Iterates over the pages in the block
    {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa); // Gets the pointer to the page
        /* there shouldn't be any free page in victim blocks */
        ftl_assert(pg_iter->status != PG_FREE); // Asserts that the page is either valid or invalid
        if (pg_iter->status == PG_VALID)        // If the page is valid, it needs to be moved
        {
            ++pages_moved;          // Increments the valid page count moved
            gc_read_page(ssd, ppa); // Simulates the read operation
            /* delay the maptbl update until "write" happens */
            gc_write_page(ssd, ppa); // Simulates the write operation
            cnt++;
        }
    }

    ftl_assert(get_blk(ssd, ppa)->vpc == cnt);
}

static void mark_line_free(struct ssd *ssd, struct ppa *ppa)
{
    struct line_mgmt *lm = &ssd->lm;
    struct line *line = get_line(ssd, ppa);
    line->ipc = 0;
    line->vpc = 0;
    /* move this line to free line list */
    QTAILQ_INSERT_TAIL(&lm->free_line_list, line, entry);
    lm->free_line_cnt++;
}

static int do_gc(struct ssd *ssd, bool force)
{
    struct line *victim_line = NULL;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lunp;
    struct ppa ppa;
    int ch, lun;

    victim_line = select_victim_line(ssd, force); // Selects the victim line
    if (!victim_line)
    {
        return -1;
    }

    ppa.g.blk = victim_line->id; // Sets the block index to the id of the victim line
    ftl_debug("GC-ing line:%d,ipc=%d,victim=%d,full=%d,free=%d\n", ppa.g.blk,
              victim_line->ipc, ssd->lm.victim_line_cnt, ssd->lm.full_line_cnt,
              ssd->lm.free_line_cnt);

    /* copy back valid data */
    for (ch = 0; ch < spp->nchs; ch++) // Iterates over the channels
    {
        for (lun = 0; lun < spp->luns_per_ch; lun++) // Iterates over the LUNs
        {
            // Sets the channel, LUN, and plane indices
            ppa.g.ch = ch;
            ppa.g.lun = lun;
            ppa.g.pl = 0;
            lunp = get_lun(ssd, &ppa);  // Returns the pointer to the LUN
            clean_one_block(ssd, &ppa); // Cleans the specified block
            mark_block_free(ssd, &ppa); // Marks the block as free

            if (spp->enable_gc_delay) // If GC delay is enabled (default is true)
            {
                struct nand_cmd gce;
                gce.type = GC_IO;
                gce.cmd = NAND_ERASE;
                gce.stime = 0;
                ssd_advance_status(ssd, &ppa, &gce); // Updates the status of the SSD
            }

            lunp->gc_endtime = lunp->next_lun_avail_time; // Updates gc_endtime of the LUN
        }
    }

    /* update line status */
    mark_line_free(ssd, &ppa); // Marks the line as free

    return 0;
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs)
    {
        ftl_err("start_lpn=%" PRIu64 ",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) // Iterates over the LPNs
    {
        ppa = get_maptbl_ent(ssd, lpn);                 // Gets the PPA using the LPN from the mapping table
        if (!mapped_ppa(&ppa) || !valid_ppa(ssd, &ppa)) // If the PPA is not mapped or invalid
        {
            // printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            // printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            // ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }

        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ssd, &ppa, &srd); // Calculates the latency of the NAND command
        maxlat = (sublat > maxlat) ? sublat : maxlat; // Updates maxlat
    }

    return maxlat; // Returns the maximum latency
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct ssdparams *spp = &ssd->sp;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= spp->tt_pgs)
    {
        ftl_err("start_lpn=%" PRIu64 ",tt_pgs=%d\n", start_lpn, ssd->sp.tt_pgs);
    }

    while (should_gc_high(ssd)) // Repeats while high threshold GC is required
    {
        r = do_gc(ssd, true); // Performs GC
        if (r == -1)          // If there is no victim line
            break;
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) // Iterates over the LPNs
    {
        ppa = get_maptbl_ent(ssd, lpn); // Gets the PPA using the LPN from the mapping table
        if (mapped_ppa(&ppa))           // If the PPA is mapped
        {
            /* update old page information first */
            mark_page_invalid(ssd, &ppa);         // Marks the page as invalid
            set_rmap_ent(ssd, INVALID_LPN, &ppa); // Removes the mapping of the PPA from the LPN in the reverse mapping table
        }
        ++ssd->page_write_counts[lpn]; // Increments the write count of the page
        ++pages_written_by_host;       // Increments the total write count

        /* new write */
#ifdef HOT_COLD_DATA_SEPARATION
        bool is_hot = is_hot_data(ssd, lpn); // Checks if the data is hot or cold
        ppa = get_new_page(ssd, is_hot);     // Gets a new page
#else
        ppa = get_new_page(ssd); // Gets a new page
#endif
        /* update maptbl */
        set_maptbl_ent(ssd, lpn, &ppa); // Sets LPN->PPA mapping in the mapping table
        /* update rmap */
        set_rmap_ent(ssd, lpn, &ppa); // Sets PPA->LPN mapping in the reverse mapping table

        mark_page_valid(ssd, &ppa); // Marks the page as valid

        /* need to advance the write pointer here */
#ifdef HOT_COLD_DATA_SEPARATION
        ssd_advance_write_pointer(ssd, is_hot); // Advances the write pointer
#else
        ssd_advance_write_pointer(ssd); // Advances the write pointer
#endif

        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ssd, &ppa, &swr); // Calculates the latency of the NAND command
        maxlat = (curlat > maxlat) ? curlat : maxlat; // Updates maxlat
    }

    return maxlat; // Returns the maximum latency
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct ssd *ssd = n->ssd;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(ssd->dataplane_started_ptr))
    {
        usleep(100000);
    }

    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;

    while (1)
    {
        for (i = 1; i <= n->nr_pollers; i++) // Iterates over the pollers (default nr_pollers is 1)
        {
            if (!ssd->to_ftl[i] || !femu_ring_count(ssd->to_ftl[i])) // If the queue does not exist or is empty
                continue;

            rc = femu_ring_dequeue(ssd->to_ftl[i], (void *)&req, 1); // Dequeues a request from the queue
            if (rc != 1)
            {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);
            switch (req->cmd.opcode)
            {
            case NVME_CMD_WRITE:
                lat = ssd_write(ssd, req); // Simulates the write operation
                break;
            case NVME_CMD_READ:
                lat = ssd_read(ssd, req); // Simulates the read operation
                break;
            case NVME_CMD_DSM:
                lat = 0;
                break;
            default:
                // ftl_err("FTL received unkown request type, ERROR\n");
                ;
            }

            req->reqlat = lat;       // Sets the latency of the request
            req->expire_time += lat; // Updates the expiration time of the request

            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1); // Enqueues the request to to_poller
            if (rc != 1)
            {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            if (should_gc(ssd)) // If GC is required
            {
                do_gc(ssd, false); // Performs GC
            }
        }
    }

    return NULL;
}
