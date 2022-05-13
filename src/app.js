const express = require('express');
const bodyParser = require('body-parser');
const {sequelize} = require('./model')
const { Op, Transaction, literal } = require('sequelize');
const {getProfile} = require('./middleware/getProfile')
const app = express();
app.use(bodyParser.json());
app.set('sequelize', sequelize)
app.set('Transaction', Transaction)
app.set('models', sequelize.models)


/**
 * Get's a contract by ID. Will only get contracts associated to passed profile_id in the headers.
 * @returns contract by id
 */
 app.get('/contracts/:id', getProfile, async (req, res) =>{
    const {Contract} = req.app.get('models')
    const {id} = req.params
    const profileId = req.profile.id

    const contract = await Contract.findOne({where: {
        id,
        [Op.or]: [
            { ContractorId: profileId },
            { ClientId: profileId }
          ]
    }})

    if(!contract) return res.status(404).end()
    res.json(contract)
})

/**
 * Get's a list of contracts associated to the profile_id passed. Only contains non-terminated contracts
 * @returns A list of non-terminated contracts
 */
 app.get('/contracts', getProfile, async (req, res) =>{
    const {Contract} = req.app.get('models')
    const profileId = req.profile.id

    const contracts = await Contract.findAll({where: {
        status: ['new','in_progress'],
        [Op.or]: [
            { ContractorId: profileId },
            { ClientId: profileId }
        ]
    }})

    res.json(contracts)
})

/**
 * Get all unpaid jobs for a user, active contracts only
 * @returns A list jobs with active contracts
 */
 app.get('/jobs/unpaid', getProfile, async (req, res) =>{
    const {Job, Contract} = req.app.get('models')
    const profileId = req.profile.id

    const jobs = await Job.findAll({
        where: {
            paid: false
        },
        include: [{
            model: Contract,
            where: {
                status: 'in_progress',
                [Op.or]: [
                    { ContractorId: profileId },
                    { ClientId: profileId }
                ]
            }
        }]
    })

    res.json(jobs)
})

/**
 * Pay for a job.
 * @returns The job requested to be paid
 */
 app.post('/jobs/:job_id/pay', getProfile, async (req, res) =>{
    const {Job, Contract, Profile} = req.app.get('models')
    const sequelize = req.app.get('sequelize')
    const Transaction = req.app.get('Transaction')
    const {job_id} = req.params
    const profile = req.profile

    const job = await Job.findOne({
        where: {
            id: job_id,
            paid: false
        },
        include: [{
            model: Contract,
            where: {
                ClientId: profile.id 
            }
        }]
    })

    if(!job) return res.status(404).end()

    // the payment cannot be processed if the client doesn't have the necessary funds
    if(job.price > profile.balance) {
        return res.status(422).send({ errors: [
                {
                    message: `Job: ${job_id}, cannot be paid because the price exceeds the client's balance.`
                }
            ] 
        })
    }
    
    // The amount should be moved from the client's balance to the contractor balance.
    try {
        await sequelize.transaction({ isolationLevel: Transaction.ISOLATION_LEVELS.SERIALIZABLE }, async (t) => {
            // Update the profile subtracting the job price from the balance
            const updatedClientRows = await Profile.update(
                {
                    balance: literal(`balance - ${job.price}`),
                },
                {
                    where: { 
                        [Op.and]: {
                            id: job.Contract.ClientId,
                            balance: {
                                [Op.gte]: job.price
                            }
                        }
                    }
                },
                { transaction: t }
            );

            if(updatedClientRows[0] !== 1) {
                throw new Error(`Failed to successfully update Client: ${updatedClientRows[0]} rows updated`)
            }

            // Update the contractor profile adding the job price to their balance
            const updatedContractorRows = await Profile.update(
                {
                    balance: literal(`balance + ${job.price}`),
                },
                {
                    where: {
                        id: job.Contract.ContractorId
                    }
                },
                { transaction: t }
            );

            if(updatedContractorRows[0] !== 1) {
                throw new Error(`Failed to successfully update Contractor: ${updatedContractorRows[0]} rows updated`)
            }

            // Update job to paid
            job.paid = true;
            job.paymentDate = new Date();
            await job.save();
        
            return job;
        });
    } catch (error) {
        console.log("Error paying job, rolling back")
        console.error(error)
        return res.status(500).send({ errors: [
                {
                    message: `Job: ${job_id}, cannot be paid due to an unknown error.`
                }
            ] 
        })
    }

    res.json(job)
})

/**
*  Deposit funds toward a client profile
*  @returns The profile deposited to
*/
app.post('/balances/deposit/:userId', getProfile, async (req, res) =>{
    const {Job, Contract} = req.app.get('models')
    const {userId} = req.params
    const amount = req.body.amount
    const profile = req.profile

    // check to make sure the user requesting the deposit is also the target user for security purposes
    if(profile.id != userId) {
        return res.status(400).send({ errors: [
                {
                    message: `User: ${userId}, does not match requesting user ID: ${profile.id}`
                }
            ] 
        })
    }

    // Find the sum of all jobs
    const jobs = await Job.findAll({
        attributes: [[sequelize.fn('sum', sequelize.col('price')), 'total']],
        where: {
            paid: false
        },
        include: [{
            model: Contract,
            where: {
                ClientId: profile.id 
            }
        }],
        group : ['Contract.ClientId']
    })

    const depositThreshold = jobs[0].dataValues.total * 0.25
    if(depositThreshold < amount) {
        return res.status(422).send({ errors: [
                {
                    message: `User: ${userId}, cannot be deposited to with an amount over ${depositThreshold}`
                }
            ] 
        })
    } else {
        profile.balance += amount
        await profile.save()
    }

    res.json(profile)
})


/**
*  Returns the profession that earned the most money (sum of jobs paid) for any contactor that worked in the query time range.
*  @returns The profession name
*/
app.get('/admin/best-profession', getProfile, async (req, res) =>{
    const { Job, Contract, Profile } = req.app.get('models')
    const { start, end } = req.query
    const errors = [];

    if (Date.parse(start) === "Invalid Date") {
        errors.push({ message: `Parameter "start", value: ${start} is not a valid date` });
    }

    if (Date.parse(end) === "Invalid Date") {
        errors.push({ message: `Parameter "end", value: ${end} is not a valid date` });
    }

    if (errors.length) {
        return res.status(400).send({ errors });
    }

    const parsedStart = new Date(start)
    const parsedEnd = new Date(end)

    try {
        const highestContractor = await Job.findOne({
            attributes: ['Contract.ContractorId', [sequelize.fn('sum', sequelize.col('price')), 'total']],
            include: [{
                model: Contract,
                required: true
            }],
            where: {
                [Op.and]: {
                    paymentDate: {[Op.between]: [parsedStart, parsedEnd]},
                    paid: true
                }
            },
            group: ['Contract.ContractorId'],
            limit: 1,
            order: sequelize.literal('total DESC')
        })

        // No contractors found with jobs completed within dates
        if (!highestContractor) return res.status(404).end()

        const profession = await Profile.findOne({
            attributes: ['profession'],
            where: {
                id: highestContractor.Contract.ContractorId
            }
        })

        res.json({ profession: profession.profession })

    } catch (error) {
        console.log("Error fetching highest profession")
        console.error(error)
        res.status(500).json([error])
    }
})

/**
*  Returns the clients with the max paid amounts on jobs in the given date range
*  @returns The profile id, full name, and paid amount objects in an array
*/
app.get('/admin/best-clients', getProfile, async (req, res) =>{
    const { Job, Contract, Profile } = req.app.get('models')
    const { start, end, limit=2 } = req.query
    const errors = [];

    if (Date.parse(start) === "Invalid Date") {
        errors.push({ message: `Parameter "start", value: ${start} is not a valid date` });
    }

    if (Date.parse(end) === "Invalid Date") {
        errors.push({ message: `Parameter "end", value: ${end} is not a valid date` });
    }

    if (!Number.isInteger(parseInt(limit)) || limit < 1) {
        errors.push({ message: `Parameter "limit", value: ${limit} is not an acceptable integer` });
    }

    if (errors.length) {
        return res.status(400).send({ errors });
    }

    const parsedStart = new Date(start)
    const parsedEnd = new Date(end)

    try {
        const bestClients = await Job.findAll({
            attributes: [[sequelize.col('Contract.Client.firstName'), 'ClientFirstName'], [sequelize.col('Contract.Client.lastName'), 'ClientLastName'], [sequelize.col('Contract.ClientId'), 'ClientId'], [sequelize.fn('max', sequelize.col('price')), 'total']],
            include: [{
                attributes: ['ClientId'],
                model: Contract,
                required: true,
                include: [{
                    model: Profile,
                    as: "Client"
                }]
            }],
            where: {
                [Op.and]: {
                    paymentDate: {[Op.between]: [parsedStart, parsedEnd]},
                    paid: true
                }
            },
            group: ['Contract.ClientId'],
            limit,
            order: sequelize.literal('total DESC')
        })

        // No contractors found with jobs completed within dates
        if (!bestClients.length) return res.status(404).end()

        res.json(bestClients.reduce((clientArray, currentClient) => {
            clientArray.push({
                id: currentClient.dataValues.ClientId,
                fullName: `${currentClient.dataValues.ClientFirstName} ${currentClient.dataValues.ClientLastName}`,
                paid: currentClient.dataValues.total
            })

            return clientArray;
        }, []));

    } catch (error) {
        console.log("Error fetching highest paying clients")
        console.error(error)
        res.status(500).json([error])
    }
})

module.exports = app;
