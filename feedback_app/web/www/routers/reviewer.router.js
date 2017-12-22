import express from 'express';

import * as ReviewerPeer from '../blockchain/reviewerPeer';

console.log('importing reviewerpeer......');
const router = express.Router();

// Render main page
router.get('/', (req, res) => {
 // res.render('reviewer-main', { reviewerActive: true });
	res.send('hello world');
});

// Feedback Processing

router.post('/api/reviews', async (req, res) => {
  let { status } = req.body;
  if (typeof status === 'string' && status[0]) {
    status = status[0].toUpperCase();
  }
  try {
    let reviews = await ReviewerPeer.getReviews(status);
    res.json(reviews);
  } catch (e) {
    res.json({ error: 'Error accessing blockchain.' });
  }
});

router.post('/api/file-review', async (req, res) => {
  if (typeof req.body.user !== 'object' ||
    typeof req.body.review != 'object') {
    res.json({ error: 'Invalid request!' });
    return;
  }

  try {
    const { user, review } = req.body;
    await ReviewerPeer.fileReview({
        date: new Date(),
        description: review.description,
        isHappy: review.isHappy
    });
    res.json({ success: true });
    return;
  } catch (e) {
    console.log(e);
    res.json({ error: 'Error accessing blockchain!' });
    return;
  }
});

router.get('*', (req, res) => {
    res.render('reviewer-main', { reviewerActive: true });
});

function wsConfig(io) {
  ReviewerPeer.on('block', block => {
    io.emit('block', block);
  });
}

export default router;
export { wsConfig };
